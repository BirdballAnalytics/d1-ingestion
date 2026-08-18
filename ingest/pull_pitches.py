"""
pull_pitches.py — Step 2 of the ingestion pipeline (run repeatedly).

Works through the `games` table's queue (status='pending'), pulling
GamePitchesTrackman for each one and writing normalized rows into
`pitches`. Designed to be run over and over (e.g. every few hours via
GitHub Actions) rather than once — a single run only processes a batch
(--limit) and stops within a time budget (--max-minutes), so a full D1
backfill happens across many runs instead of needing one long-running
job that could blow past a GitHub Actions time limit or the API's rate
limits.

What this script deliberately does NOT do:
  - called_strike_prob join: needs the umpire lookup table + (for any
    location not in it) a nearest-neighbor fill, same as build.py does
    today. Left for a separate enrichment step that runs in bulk over
    the DB rather than per-game here.
  - SDS scoring: needs the full dataset in hand to train against (same
    as today's build.py / sds_model.py). Also a separate later step.
  - AttackZone (Heart/Shadow/Chase/Waste) IS computed here, inline —
    it's pure geometry with no model dependency, cheap to do per-row.

COLUMN MAPPING CAVEAT: GamePitchesTrackman should return standard
TrackMan field names (that's the whole point of "Trackman format"),
which is what the mapping below assumes -- these are the same names the
hand-uploaded CSVs have used throughout this project. But this hasn't
been verified against a live response. On the first real run, check the
"unmapped source columns" and "target fields with no data" log lines —
those tell you exactly what to adjust in COLUMN_MAP below.
"""
import argparse
import io
import os
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from trumedia_client import TruMediaClient

# target_column: [list of possible source column names, checked in order]
COLUMN_MAP = {
    "pitch_no": ["PitchNo", "PitchNumber"],
    "date": ["Date"],
    "inning": ["Inning"],
    "top_bottom": ["Top/Bottom", "TopBottom"],
    "outs": ["Outs"],
    "balls": ["Balls"],
    "strikes": ["Strikes"],
    "pa_of_inning": ["PAofInning"],
    "pitch_of_pa": ["PitchofPA"],
    "pitcher_id": ["PitcherId", "PitcherID"],
    "pitcher": ["Pitcher"],
    "pitcher_throws": ["PitcherThrows"],
    "pitcher_team": ["PitcherTeam"],
    "batter_id": ["BatterId", "BatterID"],
    "batter": ["Batter"],
    "batter_side": ["BatterSide"],
    "batter_team": ["BatterTeam"],
    "tagged_pitch_type": ["TaggedPitchType", "AutoPitchType"],
    "pitch_call": ["PitchCall"],
    "kor_bb": ["KorBB"],
    "tagged_hit_type": ["TaggedHitType"],
    "play_result": ["PlayResult"],
    "outs_on_play": ["OutsOnPlay"],
    "runs_scored": ["RunsScored"],
    "rel_speed": ["RelSpeed"],
    "effective_velo": ["EffectiveVelo"],
    "spin_rate": ["SpinRate"],
    "spin_axis": ["SpinAxis"],
    "rel_height": ["RelHeight"],
    "rel_side": ["RelSide"],
    "extension": ["Extension"],
    "induced_vert_break": ["InducedVertBreak"],
    "horz_break": ["HorzBreak"],
    "plate_loc_height": ["PlateLocHeight"],
    "plate_loc_side": ["PlateLocSide"],
    "vert_appr_angle": ["VertApprAngle"],
    "horz_appr_angle": ["HorzApprAngle"],
    "exit_speed": ["ExitSpeed"],
    "angle": ["Angle"],
    "direction": ["Direction"],
    "distance": ["Distance"],
    "bearing": ["Bearing"],
    "contact_position_x": ["ContactPositionX"],
    "contact_position_y": ["ContactPositionY"],
    "contact_position_z": ["ContactPositionZ"],
}

STRIKE_TOP, STRIKE_BOTTOM, STRIKE_LEFT, STRIKE_RIGHT = 3.5, 1.5, -0.95, 0.95


def attack_zone(height, side):
    if pd.isna(height) or pd.isna(side):
        return None
    cx = (STRIKE_LEFT + STRIKE_RIGHT) / 2
    cy = (STRIKE_BOTTOM + STRIKE_TOP) / 2
    half_w = (STRIKE_RIGHT - STRIKE_LEFT) / 2
    half_h = (STRIKE_TOP - STRIKE_BOTTOM) / 2
    d = max(abs(side - cx) / half_w, abs(height - cy) / half_h)
    if d <= 2 / 3:
        return "Heart"
    if d <= 4 / 3:
        return "Shadow"
    if d <= 2:
        return "Chase"
    return "Waste"


def get_db():
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
    conn.autocommit = False
    return conn


def claim_pending_games(conn, limit):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select trackman_game_id, season_year from games
            where status = 'pending'
            order by game_date nulls last
            limit %s
            """,
            (limit,),
        )
        return cur.fetchall()


def mark_game(conn, trackman_game_id, status, error_message=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            update games set
                status = %s,
                error_message = %s,
                attempts = attempts + 1,
                last_attempted_at = now(),
                ingested_at = case when %s = 'success' then now() else ingested_at end
            where trackman_game_id = %s
            """,
            (status, error_message, status, trackman_game_id),
        )
    conn.commit()


def transform(df, trackman_game_id, season_year):
    unmapped_targets = []
    out = pd.DataFrame(index=df.index)
    for target, sources in COLUMN_MAP.items():
        col = next((s for s in sources if s in df.columns), None)
        if col is None:
            unmapped_targets.append(target)
            out[target] = None
        else:
            out[target] = df[col]

    out["trackman_game_id"] = trackman_game_id
    out["season_year"] = season_year
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date

    for c in ["plate_loc_height", "plate_loc_side"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["attack_zone"] = [
        attack_zone(h, s) for h, s in zip(out["plate_loc_height"], out["plate_loc_side"])
    ]

    out["raw"] = df.apply(lambda r: r.dropna().astype(str).to_dict(), axis=1)
    return out, unmapped_targets


def load_pitches(conn, df):
    if df.empty:
        return 0
    cols = list(df.columns)
    df = df.copy()
    # psycopg2 needs a plain Python dict explicitly wrapped in Json(...)
    # before it'll adapt it for a jsonb column -- otherwise it raises
    # "can't adapt type 'dict'". discover.py already does this correctly
    # for teams/games; this was the one spot that didn't.
    if "raw" in df.columns:
        df["raw"] = df["raw"].apply(lambda d: psycopg2.extras.Json(d) if d is not None else None)
    values = [tuple(row) for row in df[cols].itertuples(index=False)]
    placeholders = ", ".join(cols)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            insert into pitches ({placeholders})
            values %s
            on conflict (trackman_game_id, pitch_no) do nothing
            """,
            values,
            template=None,
        )
    conn.commit()
    return len(values)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=25, help="max games to pull this run")
    ap.add_argument("--max-minutes", type=float, default=15, help="stop pulling new games after this long")
    ap.add_argument("--sleep", type=float, default=1.0, help="seconds to wait between game pulls (politeness, beyond the client's own 429 handling)")
    args = ap.parse_args()

    client = TruMediaClient()
    conn = get_db()
    start = time.monotonic()

    games = claim_pending_games(conn, args.limit)
    print(f"Claimed {len(games)} pending games (limit={args.limit}).")

    n_success, n_error = 0, 0
    for i, g in enumerate(games):
        if (time.monotonic() - start) / 60 > args.max_minutes:
            print(f"Hit --max-minutes budget ({args.max_minutes} min); stopping, remaining games stay 'pending' for next run.")
            break

        tid = g["trackman_game_id"]
        print(f"[{i+1}/{len(games)}] Pulling {tid} ...")
        try:
            text = client.query("GamePitchesTrackman", {"trackmanGameId": tid})
            df = pd.read_csv(io.StringIO(text))
            if df.empty:
                mark_game(conn, tid, "skipped", "Empty response")
                print("  Empty response, marked skipped.")
                continue

            transformed, unmapped = transform(df, tid, g["season_year"])
            if unmapped and i == 0:
                print(f"  NOTE: these target fields found no matching source column: {unmapped}")
                print(f"        Actual columns in the response: {list(df.columns)}")

            n_rows = load_pitches(conn, transformed)
            mark_game(conn, tid, "success")
            n_success += 1
            print(f"  Inserted {n_rows} pitch rows.")
        except Exception as e:
            mark_game(conn, tid, "error", str(e)[:500])
            n_error += 1
            print(f"  ERROR: {e}")

        time.sleep(args.sleep)

    print(f"\nDone this run: {n_success} games succeeded, {n_error} errored.")
    with conn.cursor() as cur:
        cur.execute("select status, count(*) from games group by status")
        print("Queue status:", dict(cur.fetchall()))
    conn.close()


if __name__ == "__main__":
    main()
