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

COLUMN MAPPING — REVISED AFTER A REAL RUN: an earlier version of this
script assumed GamePitchesTrackman returns standard TrackMan CSV export
field names (RelSpeed, PlateLocHeight, etc.), the same names the
hand-uploaded CSVs use. That assumption was wrong — TruMedia reformats
everything into its own naming convention (releaseVelocity, pzNorm/
pxNorm, x0/z0, etc.). The mapping below is built from the real column
list a live run returned. Some map with high confidence (exact
conceptual matches: extension, inducedVertBreak, horzBreak,
vertApprAngle, horzApprAngle, spinRate, exitVelocity, launchAngle).
Others are an educated guess from the field NAME alone, since only
column names were visible, not actual values, at the time this was
written — those are flagged inline. And a handful of fields the app
depends on (tagged_hit_type, runs_scored per play, effective_velo,
batted-ball direction/distance/bearing, contact position x/y/z) don't
appear in this endpoint's column list at all — worth asking TruMedia
support directly whether GamePitchesTrackman supports requesting
additional columns for batted-ball trajectory detail, since the docs'
`columns` parameter is documented mainly for aggregate query types, and
it's unclear whether it applies here too.
"""
import argparse
import io
import os
import time

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from trumedia_client import TruMediaClient

# target_column: [list of possible source column names, checked in order]
COLUMN_MAP = {
    "pitch_uid":            ["trackmanPitchUID"],
    "date":                 ["gameDate"],
    "inning":                ["inning"],
    "top_bottom":             ["side"],                    # values not yet confirmed (Top/Bottom? T/B?)
    "outs":                    ["outs"],
    "balls":                    ["balls"],
    "strikes":                   ["strikes"],
    "pa_of_inning":                ["abNumInSide"],         # at-bat number within the half-inning
    "pitch_of_pa":                   ["pitchNumInAB"],
    "pitcher_id":                     ["pitcherId"],
    "pitcher":                         ["pitcherName"],
    "pitcher_throws":                   ["pitcherHand"],
    "pitcher_team":                      ["pitchingTeamName"],
    "batter_id":                          ["batterId"],
    "batter":                              ["batterName"],
    "batter_side":                          ["batterHand"],
    "batter_team":                           ["battingTeamName"],
    "tagged_pitch_type":                      ["pitchType"],
    "pitch_call":                              ["pitchResult"],    # medium confidence -- need real values to confirm semantics
    "play_result":                                ["atBatResult"],  # CONFIRMED against real data: S/K/SH-style PA outcome codes
    "outs_on_play":                                 ["totalOuts"],   # CONFIRMED against real data: 0 on true in-play outcomes, null on fouls
    "rel_speed":                                      ["releaseVelocity"],
    "spin_rate":                                        ["spinRate"],
    "spin_axis":                                          ["spinDir"],
    "rel_height":                                           ["z0"],   # tentative -- standard TrackMan physics notation guess
    "rel_side":                                               ["x0"],  # tentative, same caveat
    "extension":                                                ["extension"],
    "induced_vert_break":                                         ["inducedVertBreak"],
    "horz_break":                                                   ["horzBreak"],
    "plate_loc_height":                                               ["pzNorm"],  # tentative -- "Norm" suggests normalized plate location
    "plate_loc_side":                                                   ["pxNorm"],  # tentative, same caveat
    "vert_appr_angle":                                                    ["vertApprAngle"],
    "horz_appr_angle":                                                      ["horzApprAngle"],
    "exit_speed":                                                             ["exitVelocity"],
    "angle":                                                                    ["launchAngle"],
    # No match found in the real response for these -- left unmapped on
    # purpose rather than guessing at a wrong column:
    #   tagged_hit_type, runs_scored, effective_velo, direction,
    #   distance, bearing, contact_position_x/y/z
}

# plate_loc_height/plate_loc_side (pzNorm/pxNorm) are confirmed NOT to be
# raw feet -- a real pulled row had plate_loc_height = -0.928, which is
# impossible for a literal height above the ground. "Norm" evidently means
# normalized to some scale we haven't confirmed yet (a fixed generic zone?
# a batter-specific dynamic one? something else?). The raw pzNorm/pxNorm
# values are still stored in these columns below since they're real,
# useful data -- just don't treat them as feet until TruMedia confirms
# the actual normalization. See the note in the README about the
# question worth asking their support team directly.

STRIKE_TOP, STRIKE_BOTTOM, STRIKE_LEFT, STRIKE_RIGHT = 3.5, 1.5, -0.95, 0.95
# (no longer used by attack_zone() now that plate_loc_height/side are
#  confirmed pre-normalized -- left in case a future field genuinely
#  needs the raw-feet zone definition)


def attack_zone(height_norm, side_norm):
    """height_norm/side_norm: plate_loc_height/plate_loc_side (pzNorm/
    pxNorm), confirmed against real data to already be normalized to the
    strike zone -- roughly 0 = center, 1 = edge (called strikes had a
    median max-distance of 0.87, called balls 1.86, both consistent with
    that scale). Bucket directly with Savant's Heart(<=2/3)/
    Shadow(<=4/3)/Chase(<=2)/Waste(>2) thresholds -- no feet conversion
    needed since these are already normalized, unlike the first version
    of this function which wrongly assumed raw feet."""
    if pd.isna(height_norm) or pd.isna(side_norm):
        return None
    d = max(abs(height_norm), abs(side_norm))
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
    # Caller is responsible for rolling back any failed transaction on
    # this connection first -- see the fix in main()'s except block.
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
    # pitch_no: no true sequence field exists in the real response: use
    # row position in the pulled dataframe as a best-effort display value.
    # NOT used for deduplication -- pitch_uid handles that.
    out["pitch_no"] = range(1, len(out) + 1)

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date

    # kor_bb: play_result (atBatResult) carries EVERY plate-appearance
    # outcome (confirmed against real data: singles, strikeouts, sac
    # hits, etc. all come through the same field), but kor_bb specifically
    # means "was this a strikeout or a walk, blank otherwise" -- the old
    # app's narrower convention. "K" is confirmed correct from a real
    # pulled row; "BB" for walk is an educated guess not yet confirmed
    # against real data (no walk appeared in the sample checked so far).
    KOR_BB_CODES = {"K", "BB"}
    out["kor_bb"] = out["play_result"].where(out["play_result"].isin(KOR_BB_CODES), None)

    for c in ["plate_loc_height", "plate_loc_side"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    # attack_zone: re-enabled. Confirmed empirically against 12k+ real
    # pitches with known outcomes -- called balls had a median max-
    # distance of 1.86, called strikes 0.87, both consistent with
    # plate_loc_height/side already being normalized to ~0=center,
    # ~1=zone edge. No feet conversion needed; attack_zone() buckets
    # directly off these values now.
    out["attack_zone"] = [
        attack_zone(h, s) for h, s in zip(out["plate_loc_height"], out["plate_loc_side"])
    ]

    out["raw"] = df.apply(lambda r: r.dropna().astype(str).to_dict(), axis=1)

    # Convert every NaN/NaT throughout the frame to a real Python None.
    # psycopg2 doesn't reliably adapt pandas' NaN/NaT sentinels -- a NaT
    # in particular gets stringified to the literal text "NaT", which
    # Postgres then rejects as an invalid date. This is a blanket fix
    # rather than a per-column one, since the same failure mode could
    # show up on any numeric/date column depending on what's missing in
    # a given API response.
    out = out.astype(object).where(pd.notnull(out), None)
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
            on conflict (pitch_uid) do nothing
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
            if n_success == 0 and n_error == 0:
                if unmapped:
                    print(f"  NOTE: these target fields found no matching source column: {unmapped}")
                print(f"        Actual columns in the response: {list(df.columns)}")
                print(f"        Sample row (actual values): {df.iloc[0].to_dict()}")

            n_rows = load_pitches(conn, transformed)
            mark_game(conn, tid, "success")
            n_success += 1
            print(f"  Inserted {n_rows} pitch rows.")
        except Exception as e:
            conn.rollback()  # required before this connection can run another query -- see the bug this fixes in the README/chat log
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

