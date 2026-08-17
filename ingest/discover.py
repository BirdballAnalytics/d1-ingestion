"""
discover.py — Step 1 of the ingestion pipeline.

Pulls the full list of D1 teams and games for a given season from
TruMedia (AllTeams / AllGames) and upserts them into Supabase. This is
what builds the "work queue" the pull_pitches.py script consumes —
every game lands in the `games` table with status='pending', and each
scheduled run of pull_pitches.py chips away at that queue.

Run this once per season to seed the queue, and periodically during the
season to pick up newly-played games.

NOTE ON THE D1 FILTER: the API docs' own example filters a PLAYER-level
query using `season.seasonLevel IN ('BBC','SFT') AND team.game.gameLeague
= 'D1'`. This script applies the same idea to a GAME-level query, but the
exact field path (team.game.gameLeague vs. game.gameLeague, etc.) could
plausibly differ at that level — I couldn't verify this against a live
call. The script prints how many teams/games it found after filtering;
if that number looks obviously wrong (e.g. way more than the ~300 D1
programs that exist, or zero), the filter string below is the first
thing to check and adjust.
"""
import argparse
import io
import os
import sys
import urllib.parse

import pandas as pd
import psycopg2
import psycopg2.extras

from trumedia_client import TruMediaClient

D1_FILTER = "&filters=((season.seasonLevel%20IN%20('BBC'%2C'SFT'))%20AND%20(game.gameLeague%20%3D%20'D1'))"


def get_db():
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
    conn.autocommit = False
    return conn


def fetch_csv(client, data_format, params):
    text = client.query(data_format, params)
    return pd.read_csv(io.StringIO(text))


def upsert_teams(conn, teams_df):
    if teams_df.empty:
        return 0
    rows = []
    for _, r in teams_df.iterrows():
        team_id = str(r.get("teamId") or r.get("id") or "").strip()
        if not team_id:
            continue
        rows.append((
            team_id,
            r.get("teamName") or r.get("name"),
            r.get("conference"),
            "D1",
            psycopg2.extras.Json(r.dropna().to_dict()),
        ))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            insert into teams (team_id, team_name, conference, division, raw)
            values %s
            on conflict (team_id) do update set
                team_name = excluded.team_name,
                conference = excluded.conference,
                raw = excluded.raw,
                updated_at = now()
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_games(conn, games_df, season_year):
    if games_df.empty:
        return 0
    rows = []
    for _, r in games_df.iterrows():
        trackman_id = r.get("trackmanGameId") or r.get("trackmanGameID")
        if not trackman_id or (isinstance(trackman_id, float) and pd.isna(trackman_id)):
            continue  # skip games with no trackman ID -- nothing to pull for them
        rows.append((
            str(trackman_id),
            str(r.get("gameId") or r.get("id") or ""),
            season_year,
            r.get("date") or r.get("gameDate"),
            str(r.get("homeTeamId") or r.get("homeTeam") or "") or None,
            str(r.get("awayTeamId") or r.get("awayTeam") or "") or None,
            r.get("gameLeague", "D1"),
            psycopg2.extras.Json(r.dropna().astype(str).to_dict()),
        ))
    if not rows:
        print("  No rows had a trackmanGameId -- check the actual AllGames column names (see printed sample below).")
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            insert into games (trackman_game_id, game_id, season_year, game_date,
                                home_team_id, away_team_id, game_league, raw)
            values %s
            on conflict (trackman_game_id) do nothing
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True, help="e.g. 2026")
    args = ap.parse_args()

    client = TruMediaClient()
    conn = get_db()

    print(f"Fetching AllTeams for {args.season}...")
    teams_df = fetch_csv(client, "AllTeams", {"seasonYear": args.season}) 
    print(f"  Raw AllTeams columns: {list(teams_df.columns)}")
    print(f"  {len(teams_df)} teams returned before any D1 filtering.")
    n_teams = upsert_teams(conn, teams_df)
    print(f"  Upserted {n_teams} teams.")

    print(f"\nFetching AllGames for {args.season} (D1 filter applied)...")
    try:
        games_df = fetch_csv(client, "AllGames", {"seasonYear": args.season, "filters": D1_FILTER})
    except Exception as e:
        print(f"  D1-filtered AllGames call failed ({e}); falling back to unfiltered pull so you can inspect real columns/fields.")
        games_df = fetch_csv(client, "AllGames", {"seasonYear": args.season})
    print(f"  Raw AllGames columns: {list(games_df.columns)}")
    print(f"  {len(games_df)} games returned.")
    if len(games_df):
        print(f"  Sample row: {games_df.iloc[0].to_dict()}")
    n_games = upsert_games(conn, games_df, args.season)
    print(f"  Queued {n_games} new games (status='pending'). Existing games left untouched.")

    conn.close()


if __name__ == "__main__":
    main()
