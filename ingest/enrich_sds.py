"""
enrich_sds.py — computes Swing Decision Score (SDS) for every eligible
pitch already in the `pitches` table, and writes it back.

This is the D1/Supabase port of the original sds_model.py (which itself
ported the original compute_sds.r). Key differences from both:

  - Runs as a standalone batch job against the live database, not as
    part of the per-game ingestion in pull_pitches.py -- same reasoning
    as before: needs the full dataset in hand to train against, not one
    game at a time.

  - called_strike_prob is no longer looked up from a static CSV keyed on
    raw-feet coordinates (that file's coordinate system doesn't match
    this pipeline's normalized plate_loc_height/plate_loc_side at all --
    joining them would silently produce nonsense). Instead it's trained
    directly from real labeled outcomes already in the database: every
    pitch_call of 'SL' (called strike) or 'B' (ball) is a real umpire
    decision at a known location. A k-nearest-neighbors model learns
    directly from those, no external file needed.

  - EffectiveVelo isn't available from this API (confirmed missing
    during the original column-mapping work) -- rel_speed substitutes
    for it as a swing/contact/hard-hit model predictor.

  - plate_loc_height/plate_loc_side are already normalized (0=center,
    ~1=edge), confirmed empirically earlier in this project -- so the
    dynamic in-zone feature is just |value| <= count-adjusted factor,
    no raw-feet zone constants needed.

Run this as a scheduled/manual batch job (same pattern as the other
scripts in this repo) -- NOT on every ingestion run.
"""
import argparse
import os
import time

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier

SEED = 2025
RF_TREES = 300
PREDICTORS = ["rel_speed", "induced_vert_break", "horz_break", "plate_loc_height", "plate_loc_side", "in_zone"]
TWO_STRIKE_WEIGHT = 1.5


def get_db():
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
    conn.autocommit = False
    return conn


def load_pitches(conn, limit=None):
    print("Loading pitches from Supabase...")
    query = """
        select pitch_uid, balls, strikes, pitch_call, rel_speed, induced_vert_break,
               horz_break, plate_loc_height, plate_loc_side, exit_speed
        from pitches
        where plate_loc_height is not null and plate_loc_side is not null
    """
    if limit:
        query += f" limit {int(limit)}"
    df = pd.read_sql(query, conn)
    print(f"  Loaded {len(df)} rows.")
    return df


def add_dynamic_in_zone(df):
    factor = np.select(
        [df["strikes"] == 0, df["strikes"] == 1, df["strikes"] == 2],
        [0.9, 1.0, 1.15], default=1.0,
    )
    df["in_zone"] = ((df["plate_loc_height"].abs() <= factor) & (df["plate_loc_side"].abs() <= factor)).astype(int)
    return df


def classify_outcomes(df):
    df["swing"] = df["pitch_call"].isin(["SS", "F", "IP"]).astype(int)
    df["contact"] = df["pitch_call"].isin(["F", "IP"]).astype(int)
    df["hard_hit"] = ((df["pitch_call"] == "IP") & (df["exit_speed"].fillna(0) >= 95)).astype(int)
    return df


def fit_called_strike_model(df):
    """Trained directly from real labeled outcomes (called strikes vs
    balls) already in the data -- no external lookup file."""
    print("Fitting called-strike probability model from real umpire decisions...")
    labeled = df[df["pitch_call"].isin(["SL", "B"])].dropna(subset=["plate_loc_height", "plate_loc_side"])
    print(f"  {len(labeled)} labeled called-ball/called-strike pitches to train on.")
    X = labeled[["plate_loc_height", "plate_loc_side"]].values
    y = (labeled["pitch_call"] == "SL").astype(int).values
    model = KNeighborsClassifier(n_neighbors=50, weights="distance")
    model.fit(X, y)
    all_X = df[["plate_loc_height", "plate_loc_side"]].values
    proba = model.predict_proba(all_X)
    classes = list(model.classes_)
    df["called_strike_prob"] = proba[:, classes.index(1)] if 1 in classes else 0.0
    return df


def _prob1(model, X):
    classes = list(model.classes_)
    proba = model.predict_proba(X)
    return proba[:, classes.index(1)] if 1 in classes else np.zeros(len(X))


def train_and_score(df):
    eligible = ~((df["balls"] == 3) & (df["strikes"] == 0))
    eligible &= ~((df["called_strike_prob"] < 0.01) & (df["swing"] == 0))
    model_df = df.loc[eligible].dropna(subset=PREDICTORS + ["swing", "contact", "hard_hit"]).copy()
    print(f"Eligible rows for modeling: {len(model_df)} (excludes 3-0 counts and near-zero-called-strike-prob takes)")

    if len(model_df) < 500:
        print("Not enough eligible rows to train SDS models. Aborting.")
        return None, None, None

    train, test = train_test_split(model_df, test_size=0.5, random_state=SEED, stratify=model_df["swing"])

    def rf(**kw):
        return RandomForestClassifier(n_estimators=RF_TREES, random_state=SEED, n_jobs=-1, **kw)

    swing_model = rf().fit(train[PREDICTORS], train["swing"])
    train_swings = train[train["swing"] == 1]
    contact_model = rf().fit(train_swings[PREDICTORS], train_swings["contact"])
    train_contact = train_swings[train_swings["contact"] == 1]
    train_contact_hh = train_contact[train_contact["strikes"] != 2]
    hardhit_model = rf().fit(train_contact_hh[PREDICTORS], train_contact_hh["hard_hit"])

    print("--- Held-out test set performance ---")
    try:
        auc = roc_auc_score(test["swing"], _prob1(swing_model, test[PREDICTORS]))
        print(f"Swing model    AUC: {auc:.3f}  (n={len(test)})")
    except ValueError:
        print("Swing model    AUC: skipped (only one class present)")
    test_swings = test[test["swing"] == 1]
    if len(test_swings) >= 20:
        try:
            auc = roc_auc_score(test_swings["contact"], _prob1(contact_model, test_swings[PREDICTORS]))
            print(f"Contact model  AUC: {auc:.3f}  (n={len(test_swings)})")
        except ValueError:
            print("Contact model  AUC: skipped (only one class present)")
    test_contact = test_swings[(test_swings["contact"] == 1) & (test_swings["strikes"] != 2)]
    if len(test_contact) >= 20:
        try:
            auc = roc_auc_score(test_contact["hard_hit"], _prob1(hardhit_model, test_contact[PREDICTORS]))
            print(f"Hard-hit model AUC: {auc:.3f}  (n={len(test_contact)})")
        except ValueError:
            print("Hard-hit model AUC: skipped (only one class present)")
    print("--------------------------------------")

    X_full = model_df[PREDICTORS]
    model_df["P_swing"] = _prob1(swing_model, X_full)
    model_df["P_contact_given_swing"] = _prob1(contact_model, X_full)
    model_df["P_hardhit_given_contact"] = _prob1(hardhit_model, X_full)
    model_df["P_contact_hardhit_given_swing"] = model_df["P_contact_given_swing"] * model_df["P_hardhit_given_contact"]

    model_df["raw_score"] = np.where(
        model_df["swing"] == 1,
        model_df["called_strike_prob"] + model_df["P_contact_hardhit_given_swing"],
        (1 - model_df["called_strike_prob"]) * (1 - model_df["P_swing"]),
    )
    model_df["raw_score"] = np.where(model_df["strikes"] == 2, model_df["raw_score"] * TWO_STRIKE_WEIGHT, model_df["raw_score"])

    min_raw, max_raw = model_df["raw_score"].min(), model_df["raw_score"].max()
    if max_raw - min_raw == 0:
        max_raw = min_raw + 1e-6
    model_df["sds"] = 100 * (model_df["raw_score"] - min_raw) / (max_raw - min_raw)

    league_mean = float(model_df["sds"].mean())
    league_sd = float(model_df["sds"].std())
    print(f"League SDS: mean={league_mean:.2f}, sd={league_sd:.2f}, n={len(model_df)}")
    return model_df[["pitch_uid", "sds"]], league_mean, league_sd


def write_back(conn, scored, league_mean, league_sd, batch_size=10000):
    # Batched rather than one giant UPDATE -- a single statement touching
    # 650k+ rows hit Supabase's statement timeout on the first real run
    # against the full dataset. Small batches, each committed separately,
    # stay comfortably under any reasonable timeout AND mean a failure
    # partway through doesn't lose already-written progress (unlike the
    # single-statement version, where a timeout rolled back everything).
    print(f"Writing {len(scored)} SDS scores back to pitches in batches of {batch_size}...")
    rows = list(scored.itertuples(index=False, name=None))
    total_updated = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                update pitches set sds = data.sds
                from (values %s) as data(pitch_uid, sds)
                where pitches.pitch_uid = data.pitch_uid
                """,
                batch,
                template="(%s, %s::numeric)",
            )
            total_updated += cur.rowcount
        conn.commit()
        print(f"  Batch {i // batch_size + 1}/{(len(rows) - 1) // batch_size + 1}: {total_updated}/{len(rows)} rows updated so far...")
    print(f"  Total updated: {total_updated} rows.")

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into sds_meta (id, league_mean_sds, league_sd_sds, n_pitches, computed_at)
            values (1, %s, %s, %s, now())
            on conflict (id) do update set
                league_mean_sds = excluded.league_mean_sds,
                league_sd_sds = excluded.league_sd_sds,
                n_pitches = excluded.n_pitches,
                computed_at = excluded.computed_at
            """,
            (league_mean, league_sd, len(scored)),
        )
    conn.commit()
    print("Done.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="cap rows loaded, for testing")
    args = ap.parse_args()

    start = time.monotonic()
    conn = get_db()
    df = load_pitches(conn, limit=args.limit)
    if df.empty:
        print("No pitches with location data found. Nothing to do.")
        return
    df = add_dynamic_in_zone(df)
    df = classify_outcomes(df)
    df = fit_called_strike_model(df)
    scored, league_mean, league_sd = train_and_score(df)
    if scored is not None:
        write_back(conn, scored, league_mean, league_sd)
    conn.close()
    print(f"Total time: {time.monotonic()-start:.1f}s")


if __name__ == "__main__":
    main()
