#!/usr/bin/env python3
"""
Scrape OpenSky `/flights/all` for the last 24h in 1-hour windows, filter to
DOI fleet ICAO hex codes (and optionally common military hex blocks), and
write a slim per-aircraft history JSON for the FIRESTORM frontend.

Why this exists: airplanes.live shows live transponder broadcasts only.
Many DOI Office of Aviation Services aircraft are LADD/PIA-listed and
disappear from live feeds, but their flight HISTORY is logged by OpenSky's
~5K community feeders. This pipeline answers the question "did this DOI
tail fly today" even when it's not transmitting right now.

Credit budget (4000/day):
- /flights/all 1-hour window costs ~30 credits per call (measured 2026-05-05).
- 24 calls/day = 720 credits = 18% of daily budget.
- Run via GHA cron once daily at 04:00 UTC; reads LAST 24 hours.

Output: data/aircraft_history.json
Shape: {
  generated_at: ISO8601,
  window_hours: 24,
  doi_count: int,
  flights: [
    { icao24, callsign, dep, arr, firstSeen, lastSeen, doi: bool, ... },
    ...
  ]
}

Auth: OAuth2 client credentials. CLIENT_ID + CLIENT_SECRET passed via env.
Token cached in-process (lasts 30 min).
"""

from __future__ import annotations
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

OPENSKY_TOKEN_URL = 'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token'
OPENSKY_API_BASE  = 'https://opensky-network.org/api'
WINDOW_SEC        = 3600        # 1 hour per call (max accepted by /flights/all)
TOTAL_HOURS       = 24
INTER_CALL_DELAY  = 1.0          # spacing to avoid burst-rate limiting

CLIENT_ID     = os.environ.get('OPENSKY_CLIENT_ID', '').strip()
CLIENT_SECRET = os.environ.get('OPENSKY_CLIENT_SECRET', '').strip()


def get_token() -> str:
    """OAuth2 client_credentials. Token lifetime is 1800s; we never cache
    across runs, just within a single invocation."""
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError('OPENSKY_CLIENT_ID/OPENSKY_CLIENT_SECRET not set in env')
    r = requests.post(OPENSKY_TOKEN_URL, data={
        'grant_type':    'client_credentials',
        'client_id':     CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    }, timeout=15)
    r.raise_for_status()
    return r.json()['access_token']


def load_doi_hex_map() -> dict[str, dict]:
    """Load DOI fleet → return any pre-resolved icao24 hex codes.

    Most entries won't have hex yet (FAA registry endpoint is intermittently
    503'd, blocking our N-number→hex builder). When hex is missing we still
    capture the flight in the broader log if its callsign matches a fleet
    N-number — frontend resolves owner from callsign at display time.
    """
    path = os.path.join(os.path.dirname(__file__), 'data', 'doi_fleet.json')
    try:
        with open(path) as f:
            d = json.load(f)
    except FileNotFoundError:
        return {}
    out: dict[str, dict] = {}
    for ac in d.get('aircraft', []):
        reg = (ac.get('reg') or '').strip().upper()
        hx  = (ac.get('icao24') or '').strip().lower()
        if hx:
            out[hx] = {'reg': reg, 'base': ac.get('base'), 'type': ac.get('type')}
    return out


def doi_callsigns_set() -> set[str]:
    """All DOI N-numbers as callsign candidates (uppercase, no dashes)."""
    path = os.path.join(os.path.dirname(__file__), 'data', 'doi_fleet.json')
    try:
        with open(path) as f:
            d = json.load(f)
    except FileNotFoundError:
        return set()
    out = set()
    for ac in d.get('aircraft', []):
        reg = (ac.get('reg') or '').strip().upper().replace('-', '')
        if reg:
            out.add(reg)
    return out


def fetch_window(token: str, begin: int, end: int) -> list[dict]:
    """Fetch one 1-hour /flights/all window."""
    url = f'{OPENSKY_API_BASE}/flights/all?begin={begin}&end={end}'
    r = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=30)
    if r.status_code == 404:
        # OpenSky returns 404 when no flights in window — treat as empty
        return []
    if r.status_code == 429:
        print(f'  [history] rate-limited, sleeping 30s', flush=True)
        time.sleep(30)
        return []
    if r.status_code >= 400:
        print(f'  [history] HTTP {r.status_code}', flush=True)
        return []
    rem = r.headers.get('x-rate-limit-remaining', '?')
    data = r.json() or []
    print(f'  [history] {begin}..{end}: {len(data)} flights | credits remaining: {rem}', flush=True)
    return data


def main() -> int:
    print('=' * 60)
    print('FIRESTORM OpenSky History Pipeline')
    print(f'Time: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')
    print('=' * 60)

    token = get_token()
    print('[auth] OAuth2 token acquired', flush=True)

    doi_hex_map = load_doi_hex_map()
    doi_callsigns = doi_callsigns_set()
    print(f'[doi] {len(doi_callsigns)} fleet N-numbers, {len(doi_hex_map)} pre-resolved hex codes', flush=True)

    now    = int(time.time())
    window_end = now
    all_flights: list[dict] = []
    seen_keys: set[str] = set()

    for i in range(TOTAL_HOURS):
        window_begin = window_end - WINDOW_SEC
        flights = fetch_window(token, window_begin, window_end)
        for fl in flights:
            hx = (fl.get('icao24') or '').lower()
            cs = (fl.get('callsign') or '').strip().upper()
            # Dedupe across overlapping window edges (firstSeen is unique per flight)
            key = f"{hx}_{fl.get('firstSeen')}"
            if key in seen_keys:
                continue
            is_doi = (hx in doi_hex_map) or (cs in doi_callsigns)
            # Drop everything that isn't DOI to keep the file small. We can
            # add a second pass for military hex blocks later if useful.
            if not is_doi:
                continue
            seen_keys.add(key)
            doi_meta = doi_hex_map.get(hx) or {}
            all_flights.append({
                'icao24':     hx,
                'callsign':   cs,
                'reg':        doi_meta.get('reg') or cs,
                'base':       doi_meta.get('base'),
                'type':       doi_meta.get('type'),
                'dep':        fl.get('estDepartureAirport'),
                'arr':        fl.get('estArrivalAirport'),
                'firstSeen':  fl.get('firstSeen'),
                'lastSeen':   fl.get('lastSeen'),
                'duration_s': (fl.get('lastSeen') or 0) - (fl.get('firstSeen') or 0),
            })
        window_end = window_begin
        time.sleep(INTER_CALL_DELAY)

    output = {
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'window_hours': TOTAL_HOURS,
        'source':       'OpenSky Network /flights/all',
        'doi_count':    len(all_flights),
        'flights':      all_flights,
    }

    out_path = os.path.join('data', 'aircraft_history.json')
    os.makedirs('data', exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    print(f'\n[write] {out_path}: {len(all_flights)} DOI flights in last {TOTAL_HOURS}h')
    return 0


if __name__ == '__main__':
    sys.exit(main())
