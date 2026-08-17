"""
Thin client for the TruMedia baseball API.

Handles the two-step auth flow (master token -> short-lived temp token)
and wraps every request with the rate-limit handling TruMedia's own docs
ask for: single-threaded, and on a 429 response, wait for the duration in
the Retry-After header before retrying.

Credentials come from environment variables ONLY -- never hardcode a
username/sitename/token here. In GitHub Actions these are repo secrets;
locally, put them in a .env file that's in .gitignore (see README).
"""
import os
import time
import urllib.parse

import requests

BASE = "https://api.trumedianetworks.com/v1"
TOKEN_ENDPOINT = f"{BASE}/siteadmin/api/createTempPBToken"
QUERY_ENDPOINT = f"{BASE}/mlbapi/custom/baseball/DirectedQuery"

# How long a temp token is asked to live. Max 72 hours per the docs; we
# request the max so a multi-run backfill doesn't need to re-auth constantly.
TEMP_TOKEN_HOURS = 72


class TruMediaClient:
    def __init__(self, username=None, sitename=None, master_token=None):
        self.username = username or os.environ["TRUMEDIA_USERNAME"]
        self.sitename = sitename or os.environ["TRUMEDIA_SITENAME"]
        self.master_token = master_token or os.environ["TRUMEDIA_MASTER_TOKEN"]
        self._temp_token = None
        self._session = requests.Session()

    def _get_temp_token(self):
        if self._temp_token:
            return self._temp_token
        resp = self._session.post(
            TOKEN_ENDPOINT,
            headers={"Content-Type": "application/json"},
            json={
                "username": self.username,
                "sitename": self.sitename,
                "token": self.master_token,
                "expireHours": TEMP_TOKEN_HOURS,
            },
            timeout=30,
        )
        resp.raise_for_status()
        tok = resp.json().get("pbTempToken")
        if not tok:
            raise RuntimeError(f"Did not receive pbTempToken from TruMedia auth response: {resp.text[:300]}")
        self._temp_token = tok
        print("  Obtained new TruMedia temp token.")
        return tok

    def query(self, data_format, params=None, file_type="csv", max_retries=6):
        """
        Calls the DirectedQuery endpoint for the given dataFormat
        (AllGames, AllTeams, TeamGames, GamePitchesTrackman, ...).
        params is a plain dict of query params (columns/filters/etc. as
        already-encoded strings where the docs call for URL-encoding).
        Returns the raw response text (CSV or JSON depending on file_type).
        """
        params = dict(params or {})
        url = f"{QUERY_ENDPOINT}/{data_format}.{file_type}"

        for attempt in range(max_retries):
            params["token"] = self._get_temp_token()
            resp = self._session.get(url, params=params, timeout=120)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "30"))
                print(f"  429 rate-limited on {data_format}; sleeping {retry_after}s (attempt {attempt+1}/{max_retries})")
                time.sleep(retry_after)
                continue

            if resp.status_code in (401, 403):
                # Temp token likely expired or invalid -- mint a fresh one and retry once.
                print("  Auth error, refreshing temp token and retrying...")
                self._temp_token = None
                continue

            resp.raise_for_status()
            return resp.text

        raise RuntimeError(f"Exceeded max retries calling {data_format} with params {params}")


def encode_columns(columns):
    """columns: list of stat abbreviations, e.g. ['[IP]','[ERA]'] or raw strings."""
    return urllib.parse.quote(",".join(columns))
