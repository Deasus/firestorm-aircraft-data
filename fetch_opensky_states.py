#!/usr/bin/env python3
"""
Pull a single global /states/all snapshot from OpenSky and emit a slim
JSON the FIRESTORM frontend can merge with airplanes.live data to widen
coverage (OpenSky's ~5K feeders catch ~40% more aircraft than
airplanes.live in our measurements).

Cost: 4 credits per call. Run from GHA cron every 30 min = 192/day = 5%
of the 4000-credit daily budget. Leaves headroom for the history scraper
and ad-hoc lookups.

Output: data/opensky_states.json
Shape: {
  generated_at: ISO8601,
  total: int,
  source: 'OpenSky /states/all',
  aircraft: [
    { hex, callsign, lat, lon, alt_baro, gs, track, on_ground, country }, ...
  ]
}

Frontend merges by ICAO hex — if a hex is in both feeds, airplanes.live
wins (richer fields). OpenSky-only entries fill the gaps.
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

CLIENT_ID     = os.environ.get('OPENSKY_CLIENT_ID', '').strip()
CLIENT_SECRET = os.environ.get('OPENSKY_CLIENT_SECRET', '').strip()


def get_token() -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError('OPENSKY_CLIENT_ID/OPENSKY_CLIENT_SECRET not set in env')
    r = requests.post(OPENSKY_TOKEN_URL, data={
        'grant_type':    'client_credentials',
        'client_id':     CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    }, timeout=15)
    r.raise_for_status()
    return r.json()['access_token']


def main() -> int:
    print('=' * 60)
    print('FIRESTORM OpenSky States Pipeline')
    print(f'Time: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')
    print('=' * 60)

    token = get_token()
    print('[auth] OAuth2 token acquired', flush=True)

    r = requests.get(f'{OPENSKY_API_BASE}/states/all',
                     headers={'Authorization': f'Bearer {token}'}, timeout=45)
    rem = r.headers.get('x-rate-limit-remaining', '?')
    r.raise_for_status()
    data = r.json()
    states = data.get('states') or []
    print(f'[states] {len(states)} aircraft | credits remaining: {rem}', flush=True)

    # OpenSky state-vector tuple per docs:
    # 0:icao24 1:callsign 2:origin_country 3:time_position 4:last_contact
    # 5:longitude 6:latitude 7:baro_altitude 8:on_ground 9:velocity
    # 10:true_track 11:vertical_rate 12:sensors 13:geo_altitude
    # 14:squawk 15:spi 16:position_source
    aircraft = []
    for s in states:
        if s[5] is None or s[6] is None:
            continue
        aircraft.append({
            'hex':       (s[0] or '').lower(),
            'callsign':  (s[1] or '').strip(),
            'country':   s[2],
            'lat':       s[6],
            'lon':       s[5],
            'alt_baro':  s[7],
            'on_ground': s[8],
            'gs':        s[9],
            'track':     s[10],
            'squawk':    s[14],
            'last_seen': s[4],
        })

    output = {
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'source':       'OpenSky /states/all',
        'total':        len(aircraft),
        'credits_remaining': rem,
        'aircraft':     aircraft,
    }

    out_path = os.path.join('data', 'opensky_states.json')
    os.makedirs('data', exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    print(f'[write] {out_path}: {len(aircraft)} aircraft')
    return 0


if __name__ == '__main__':
    sys.exit(main())
