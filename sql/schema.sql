-- ================================================================
-- D1 Baseball Database — Supabase/Postgres schema
-- ================================================================
-- Run this once in the Supabase SQL editor (Project → SQL Editor →
-- New query → paste → Run) to set up the tables the ingestion
-- pipeline writes to and the frontend will eventually read from.
--
-- Design notes:
--   - `pitches` has typed columns for the fields the existing app
--     already depends on (these are standard TrackMan export field
--     names, which is what GamePitchesTrackman returns per the API
--     docs — the same format the hand-uploaded CSVs have used all
--     along). It ALSO has a `raw` JSONB column holding the complete,
--     untouched API response for that row. That's deliberate: since
--     I can't verify the API's exact column names against a live
--     pull from here, the typed columns are a best-effort mapping
--     that may need small adjustments after the first real run, and
--     `raw` is the safety net that means no data is lost if a typed
--     column mapping needs fixing later — you're never stuck
--     re-pulling from the API to recover a field that got missed.
--   - `games` tracks *ingestion status* per game (pending/success/
--     error), which is what makes the pull resumable across many
--     GitHub Actions runs instead of needing one giant run that
--     could easily exceed a job's time limit or the API's rate
--     limits partway through.
--   - Indexes are chosen for the query patterns the current app
--     already uses: filter by team, by date range, by game.
-- ================================================================

create table if not exists teams (
    team_id         text primary key,       -- TruMedia teamId
    team_name       text,
    conference      text,
    division        text,                   -- e.g. 'D1'
    raw             jsonb,
    updated_at      timestamptz not null default now()
);

create table if not exists games (
    trackman_game_id    text primary key,   -- e.g. '20260418-HarringtonVillage-2'
    game_id              text,              -- TruMedia gameId (numeric-ish string)
    season_year          int,
    game_date             date,
    home_team_id          text references teams(team_id),
    away_team_id          text references teams(team_id),
    game_league           text,             -- 'D1' filter value from the API
    -- Ingestion status drives the resumable pull: a scheduled job
    -- repeatedly asks "give me N games where status = 'pending'",
    -- pulls them, and flips status to 'success' or 'error'.
    status                text not null default 'pending'
                            check (status in ('pending','success','error','skipped')),
    error_message         text,
    attempts              int not null default 0,
    last_attempted_at     timestamptz,
    ingested_at            timestamptz,
    raw                    jsonb,
    created_at              timestamptz not null default now()
);

create index if not exists idx_games_status on games(status);
create index if not exists idx_games_season on games(season_year);
create index if not exists idx_games_date on games(game_date);

create table if not exists pitches (
    id                  bigint generated always as identity primary key,
    trackman_game_id    text not null references games(trackman_game_id),
    pitch_no            int,

    -- context
    date                date,
    season_year         int,
    inning              int,
    top_bottom          text,
    outs                int,
    balls               int,
    strikes             int,
    pa_of_inning        int,
    pitch_of_pa         int,

    -- participants
    pitcher_id           text,
    pitcher              text,
    pitcher_throws        text,
    pitcher_team           text,
    batter_id               text,
    batter                   text,
    batter_side               text,
    batter_team                text,

    -- pitch classification / outcome
    tagged_pitch_type            text,
    pitch_call                     text,
    kor_bb                          text,
    tagged_hit_type                   text,
    play_result                        text,
    outs_on_play                        numeric,
    runs_scored                           numeric,

    -- pitch physics
    rel_speed          numeric,
    effective_velo      numeric,
    spin_rate             numeric,
    spin_axis              numeric,
    rel_height               numeric,
    rel_side                  numeric,
    extension                  numeric,
    induced_vert_break            numeric,
    horz_break                      numeric,
    plate_loc_height                  numeric,
    plate_loc_side                      numeric,
    vert_appr_angle                       numeric,
    horz_appr_angle                         numeric,

    -- batted ball
    exit_speed         numeric,
    angle                numeric,
    direction              numeric,
    distance                 numeric,
    bearing                    numeric,
    contact_position_x           numeric,
    contact_position_y             numeric,
    contact_position_z               numeric,

    -- called-strike probability, joined during ingestion the same way
    -- build.py already does it for the hand-uploaded CSVs
    called_strike_prob  numeric,

    -- SDS gets computed in a later batch step, not at ingestion time
    -- (same reasoning as the current build.py pipeline: needs the
    -- full dataset in hand to train against, not one game at a time)
    sds                 numeric,
    attack_zone          text,

    raw                  jsonb not null,   -- full original API row
    ingested_at          timestamptz not null default now()
);

create index if not exists idx_pitches_game on pitches(trackman_game_id);
create index if not exists idx_pitches_date on pitches(date);
create index if not exists idx_pitches_season on pitches(season_year);
create index if not exists idx_pitches_pitcher_team on pitches(pitcher_team);
create index if not exists idx_pitches_batter_team on pitches(batter_team);
create index if not exists idx_pitches_pitcher on pitches(pitcher_id);
create index if not exists idx_pitches_batter on pitches(batter_id);
-- Prevents double-ingesting the same pitch if a game gets re-pulled
create unique index if not exists uq_pitches_game_pitchno on pitches(trackman_game_id, pitch_no);
