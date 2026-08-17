# D1 Baseball Ingestion Pipeline

This is Phase 1 of the D1 rebuild: pulling data from TruMedia into a real
database, so the frontend has something to actually query instead of a
static JSON file baked into an HTML page (which stops being viable at
D1 scale). The frontend rewrite is a **separate, later phase** — nothing
here touches `app.js`. This phase's only job is: get correct, complete
data landing reliably in Supabase.

## 1. Set up Supabase

1. Create a project at [supabase.com](https://supabase.com) (Pro tier,
   ~$25/mo — the free tier's 500MB storage is too small for a season of
   D1 pitch-level data).
2. Once it's created: **SQL Editor → New query**, paste in the contents
   of `sql/schema.sql`, run it. This creates `teams`, `games`, and
   `pitches`.
3. **Project Settings → Database → Connection string** — copy the
   **direct connection** string (not the pooler one; the pooler is meant
   for many short-lived serverless connections, not a long-running batch
   script). It looks like:
   `postgresql://postgres:[PASSWORD]@db.xxxxxxxx.supabase.co:5432/postgres`

## 2. Set GitHub repo secrets

**Settings → Secrets and variables → Actions → New repository secret.**
Add all four:

| Secret name | Value |
|---|---|
| `TRUMEDIA_USERNAME` | Your TruMedia account email |
| `TRUMEDIA_SITENAME` | Your TruMedia sitename |
| `TRUMEDIA_MASTER_TOKEN` | Your TruMedia master token |
| `SUPABASE_DB_URL` | The connection string from step 1.3 |

None of these should ever appear in a commit, a script, or a chat —
secrets are the only place they live.

**If the master token that was shared in chat with me is still the one
in use, regenerate it first** (contact `mlbsupport@trumedianetworks.com`
or `ncaasupport@trumedianetworks.com`) before wiring it in here.

## 3. Seed the queue

From the Actions tab, run the `Ingest D1 pitch data` workflow manually
(**Run workflow**), filling in a season (e.g. `2026`) in the input box.
This runs `discover.py`, which pulls every D1 team and game for that
season from TruMedia and drops them into the `games` table with
`status='pending'` — that's the work queue everything else consumes.

**Check the Actions log after this run.** It prints the *actual* column
names TruMedia's AllTeams/AllGames responses contain, and how many
teams/games came back. If that number is wildly off from what you'd
expect (D1 baseball has roughly ~300 programs), the D1 filter in
`discover.py` (`D1_FILTER`) is the first thing to check — I built it
from the one filter example in the API docs, applied to a different
query type than that example used, so it's a reasonable guess rather
than a verified one.

## 4. Let the backfill run

Once seeded, the scheduled workflow (every 3 hours, or trigger it
manually anytime) picks up 40 pending games per run and pulls
`GamePitchesTrackman` for each. **Watch the first run's log closely** —
`pull_pitches.py` prints "NOTE: these target fields found no matching
source column" if any expected TrackMan field name doesn't show up in
the real response, plus the actual column list it got back. If that
shows up, tell me what it printed and I'll fix the mapping in
`COLUMN_MAP` (in `pull_pitches.py`) — I validated that mapping against
our own real, already-uploaded TrackMan CSVs (100% of the ~45 expected
fields matched), but that's a strong signal, not a guarantee, since I
can't call TruMedia's live API myself to confirm.

A rough sense of scale: ~300 D1 programs × ~56 games/season, most
counted twice (both teams' games overlap), is very roughly 8,000-9,000
unique games for a full season. At 40 games/run, every 3 hours, that's
somewhere around 3-4 weeks to fully backfill one season from a cold
start — and that's a deliberately conservative estimate given the
"single-threaded, respect 429s" guidance in the docs. Once caught up,
ongoing runs just pick up newly-played games, which is fast.

## What's explicitly NOT done yet

- **called_strike_prob** — needs the umpire lookup + nearest-neighbor
  fill, same as `build.py` does today. Left for a bulk enrichment step
  that runs over the whole database at once rather than per-game here.
- **SDS** — needs the full dataset in hand to train the three models
  against (same reasoning as today's `sds_model.py`). Also a later,
  separate batch step.
- **AttackZone (Heart/Shadow/Chase/Waste) IS done** — it's pure
  geometry, no model dependency, so it's computed inline as each game
  is pulled.
- **The frontend** — `app.js` still expects one big in-memory array of
  every pitch. Rewriting it to query Supabase instead (and to support
  picking any D1 team, not just BC) is the next phase, once real data
  is confirmed flowing correctly here.

## Checking progress

In the Supabase SQL editor:

```sql
select status, count(*) from games group by status;
select count(*) from pitches;
```
