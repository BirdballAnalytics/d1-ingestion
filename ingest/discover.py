"""
discover.py — Step 1 of the ingestion pipeline.

Pulls the full list of teams and games for a given season from
TruMedia (AllTeams / AllGames) and upserts them into Supabase. This is
what builds the "work queue" the pull_pitches.py script consumes —
every D1 game lands in the `games` table with status='pending', and
each scheduled run of pull_pitches.py chips away at that queue.

Run this once per season to seed the queue, and periodically during the
season to pick up newly-played games.

ON THE D1 FILTER: an earlier version of this script tried to filter
AllGames server-side via the API's `filters` query parameter, guessed
from the one filter example in the docs (which was for a different
query type). That guess turned out to be invalid enough to make the API
return a 500 error, not just wrong results — so rather than guess again
against a live paid API, this version pulls AllGames unfiltered (which
works reliably) and filters to D1 in Python instead. The real response
includes `homeConference`/`awayConference` fields formatted like
"D1 Atlantic Coast Conference" — a game is kept if either team's
conference starts with "D1". A game is included if EITHER side is a D1
program, so a D1 team's non-conference games against lower levels still
get pulled — the goal is "every D1 team's full schedule," not "every
game where both sides happen to be D1."
"""
import argparse
import io
import os

import pandas as pd
import psycopg2
import psycopg2.extras

from trumedia_client import TruMediaClient


def get_db():
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
    conn.autocommit = False
    return conn


def fetch_csv(client, data_format, params):
    text = client.query(data_format, params)
    return pd.read_csv(io.StringIO(text))


def is_d1(row):
    home_conf = str(row.get("homeConference") or "").strip().upper()
    away_conf = str(row.get("awayConference") or "").strip().upper()
    return home_conf.startswith("D1") or away_conf.startswith("D1")


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
            # fullName confirmed against real data to be the actual school
            # name ("Abilene Christian University") on every row checked;
            # teamName is inconsistent -- sometimes a mascot ("Wildcats"),
            # sometimes a partial name, depending on the team. Prefer
            # fullName; fall back to teamName only if fullName is missing.
            r.get("fullName") or r.get("teamName"),
            r.get("conference"),   # not present in AllTeams today; kept in case it's added later
            None,                  # division unknown from AllTeams alone -- see note in README
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
                conference = coalesce(excluded.conference, teams.conference),
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
        game_date = r.get("gameDate") or r.get("date")
        if isinstance(game_date, str):
            game_date = game_date.split(" ")[0]  # drop the time component, keep just YYYY-MM-DD
        home_conf = str(r.get("homeConference") or "")
        away_conf = str(r.get("awayConference") or "")
        league = "D1" if (home_conf.strip().upper().startswith("D1") or away_conf.strip().upper().startswith("D1")) else None
        rows.append((
            str(trackman_id),
            str(r.get("gameId") or r.get("id") or ""),
            season_year,
            game_date,
            str(r.get("homeTeamId") or r.get("homeTeam") or "") or None,
            str(r.get("awayTeamId") or r.get("awayTeam") or "") or None,
            league,
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
    print(f"  {len(teams_df)} teams returned (all levels combined -- AllTeams has no division field to filter on).")
    n_teams = upsert_teams(conn, teams_df)
    print(f"  Upserted {n_teams} teams.")

    print(f"\nFetching AllGames for {args.season} (unfiltered, then filtered to D1 in Python)...")
    games_df = fetch_csv(client, "AllGames", {"seasonYear": args.season})
    print(f"  Raw AllGames columns: {list(games_df.columns)}")
    print(f"  {len(games_df)} games returned before D1 filtering.")
    if len(games_df):
        print(f"  Sample row: {games_df.iloc[0].to_dict()}")

    d1_games_df = games_df[games_df.apply(is_d1, axis=1)]
    print(f"  {len(d1_games_df)} games are D1 (either home or away conference starts with 'D1').")

    n_games = upsert_games(conn, d1_games_df, args.season)
    print(f"  Queued {n_games} new D1 games (status='pending'). Existing games left untouched.")

    conn.close()


if __name__ == "__main__":
    main()

