#!/usr/bin/env python3
"""
FIRESTORM aircraft pipeline — polls 21 global regions from airplanes.live
(with adsb.lol failover), merges + dedupes by ICAO hex, classifies each
aircraft into fire/military/medevac/helo/civilian, writes out a single
JSON the frontend loads via GitHub Pages.

Why this exists: airplanes.live rate-limits aggressive browser polling.
Running polls server-side once per cron cycle (every 1 min via GHA)
means 50 UAT users hit ONE JSON blob from GitHub CDN, with zero API load
per user. Same rationale as firestorm-news-data and firestorm-wind-data.

Output: data/aircraft.json
Shape: { "generated_at": ISO8601, "counts": {...}, "regions": {...}, "aircraft": [...] }
Each aircraft record carries: hex, callsign, reg, type, category (ADS-B),
lat, lng, alt, gs, track, squawk, emergency, _category (our classification),
_sourceRegion.

Requires: requests. No auth.
"""

from __future__ import annotations
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# ── Regional poll config ──────────────────────────────────────────────
# Each region is a (name, lat, lng, radiusNm) tuple. 500nm is the effective
# max airplanes.live tolerates without noticeable degradation. Coverage:
# two overlapping 500nm disks blanket CONUS, one for each major landmass.
REGIONS = [
    # NA operational priority
    ('CONUS-West',    40.0, -115.0, 500),
    ('CONUS-East',    38.0,  -87.0, 500),
    ('Alaska',        62.0, -152.0, 500),
    ('Hawaii',        20.5, -157.0, 300),
    # NA adjacent
    ('Canada-West',   55.0, -120.0, 500),
    ('Canada-East',   52.0,  -78.0, 500),
    ('Mexico',        23.0, -102.0, 500),
    # International fire partners
    ('Europe-West',   48.0,    5.0, 500),
    ('Europe-East',   46.0,   22.0, 500),
    ('Iberia',        40.0,   -4.0, 500),
    ('UK',            54.0,   -3.0, 300),
    ('Australia-E',  -33.0,  145.0, 500),
    ('Australia-W',  -27.0,  122.0, 500),
    # Global situational
    ('SouthAmerica', -15.0,  -60.0, 500),
    ('Argentina',    -35.0,  -65.0, 500),
    ('SouthAfrica',  -29.0,   25.0, 500),
    ('EastAfrica',     0.0,   35.0, 500),
    ('India',         22.0,   78.0, 500),
    ('SEAsia',         5.0,  108.0, 500),
    ('Japan',         36.0,  138.0, 500),
    ('MiddleEast',    28.0,   50.0, 500),
]

PRIMARY = 'https://api.airplanes.live/v2/point'
FAILOVER = 'https://api.adsb.lol/v2/point'
# Delay between regional polls — spreads load, avoids burst-rate-limit.
# Total 21 × 1.5s = ~32s per full cycle; GHA cron is min interval 1min so fine.
INTER_POLL_DELAY = 1.5

HEADERS = {
    'User-Agent': 'FIRESTORM-aircraft-pipeline/1.0 (https://github.com/Deasus/firestorm-aircraft-data)',
    'Accept': 'application/json',
}

# ── Classification ────────────────────────────────────────────────────
FIRE_CALL_PREFIX = [
    'TANKER', 'CDF', 'GUARD', 'FIRE', 'AIRAT', 'BOMBARD', 'LEAD', 'ATGS',
    'CALFIRE', 'USFS', 'SMOKEY',
]
FIRE_REG_HINT = ['USFS', 'CDF', 'BLM', 'CALFIRE', 'N-TANK']

import re
_T_NUM_RE = re.compile(r'^T\d{2,4}$')

MED_PREFIX = ['LIFE', 'MED', 'STAR', 'MERCY', 'ANGEL', 'GUARDIAN', 'AMBU', 'HEMS', 'TRAUMA']

MIL_PREFIX = [
    'RCH', 'REACH', 'NAVY', 'ARMY', 'PAT', 'OPEC', 'SHELL', 'FORGE',
    'BLUE', 'ECHO', 'SLAM', 'EAGLE', 'DARK', 'STEEL', 'HOIST', 'PAVE',
    'PACE', 'COBRA', 'VIPER', 'RAPTOR', 'HAWK',
]

HELO_TYPES = [
    'EC35', 'EC30', 'EC45', 'EC75', 'B06', 'B407', 'B412', 'B429',
    'AS35', 'AS50', 'AS32', 'A109', 'AW139', 'H125', 'H130', 'H145',
    'H160', 'R22', 'R44', 'R66', 'S70', 'S76', 'S92', 'UH1', 'UH60',
    'CH47', 'S64',
]


# ── DOI fleet whitelist ───────────────────────────────────────────────
# Loaded from data/doi_fleet.json. These N-numbers are pre-classified as
# 'doi' regardless of callsign / hex prefix, because DOI Office of Aviation
# Services aircraft are often LADD-listed or PIA-active and won't match
# the regular fire/military classifier rules.
def _load_doi_fleet():
    import os
    path = os.path.join(os.path.dirname(__file__), 'data', 'doi_fleet.json')
    if not os.path.exists(path):
        return set()
    try:
        with open(path) as f:
            data = json.load(f)
        regs = set()
        for ac in data.get('aircraft', []):
            r = (ac.get('reg') or '').strip().upper()
            if r:
                regs.add(r)
        print(f'[DOI] loaded {len(regs)} fleet registrations')
        return regs
    except Exception as e:
        print(f'[DOI] fleet load failed: {e}')
        return set()


DOI_FLEET_REGS = _load_doi_fleet()


def classify(ac: dict) -> str:
    """Return one of: 'doi', 'fire', 'medevac', 'military', 'helo', 'civilian'.
    DOI is checked FIRST so federal land-management aircraft are flagged
    even if their callsign would otherwise classify as civilian/fire."""
    hex_id = (ac.get('hex') or '').lower()
    reg = (ac.get('r') or '').upper()
    type_code = (ac.get('t') or '').upper()
    callsign = (ac.get('flight') or '').strip().upper()
    cat = (ac.get('category') or '').upper()
    squawk = (ac.get('squawk') or '').strip()

    # DOI fleet — registration whitelist takes precedence
    if reg and reg in DOI_FLEET_REGS:
        return 'doi'

    # FIRE — callsign prefix or reg hint.
    # T###-style tanker callsigns are gated on US N-reg, because international
    # registries T7-* (San Marino) and T-* (Bahamas) are private business jets,
    # not air tankers. Real US tankers fly as N-registered with T### callsign
    # (e.g. TANKER910 reg N910SS, T7788 reg N7788).
    for pfx in FIRE_CALL_PREFIX:
        if callsign.startswith(pfx):
            return 'fire'
    if _T_NUM_RE.match(callsign) and reg.startswith('N'):
        return 'fire'
    for rh in FIRE_REG_HINT:
        if rh in reg:
            return 'fire'

    # MEDEVAC — emergency squawks + callsign prefix
    if squawk in ('7500', '7600', '7700'):
        return 'medevac'
    for pfx in MED_PREFIX:
        if callsign.startswith(pfx):
            return 'medevac'

    # MILITARY — DoD hex ranges + callsign.
    # Authoritative US DoD ICAO allocation: ADFC00–AFFFFF (precisely defined).
    # Previous "h3 == 'adf'" caught civilian commercial regs in adf000–adfbff
    # (notably AAL hex assignments like adf068). Fix: numeric range check.
    if len(hex_id) == 6:
        try:
            hex_int = int(hex_id, 16)
            # 0xADFC00 .. 0xAFFFFF is the US DoD block per ICAO/FAA docs
            if 0xADFC00 <= hex_int <= 0xAFFFFF:
                return 'military'
        except ValueError:
            pass
    for pfx in MIL_PREFIX:
        if callsign.startswith(pfx):
            return 'military'

    # HELO — ADS-B rotorcraft category + type code
    if cat == 'A7':
        return 'helo'
    for t in HELO_TYPES:
        if type_code.startswith(t):
            return 'helo'

    return 'civilian'


# ── Fetching ──────────────────────────────────────────────────────────
def fetch_region(name: str, lat: float, lng: float, radius: int, session: requests.Session) -> list[dict]:
    """Fetch a single region. Falls back to adsb.lol if airplanes.live returns 429/5xx."""
    for base, label in [(PRIMARY, 'primary'), (FAILOVER, 'failover')]:
        url = f'{base}/{lat}/{lng}/{radius}'
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 429:
                print(f'  [{name}] {label} rate limited, sleeping 3s and trying failover…')
                time.sleep(3)
                continue
            if r.status_code >= 500:
                print(f'  [{name}] {label} HTTP {r.status_code}, trying failover…')
                continue
            r.raise_for_status()
            data = r.json()
            ac = data.get('ac') or []
            print(f'  [{name}] {len(ac)} aircraft (via {label})')
            return ac
        except requests.exceptions.Timeout:
            print(f'  [{name}] {label} timeout')
            continue
        except Exception as e:
            print(f'  [{name}] {label} failed: {e}')
            continue
    print(f'  [{name}] ALL sources failed, returning empty')
    return []


# ── Main ──────────────────────────────────────────────────────────────
def main() -> int:
    print('=' * 60)
    print('FIRESTORM Aircraft Pipeline')
    print(f'Time: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')
    print(f'Regions: {len(REGIONS)}')
    print('=' * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    registry: dict[str, dict] = {}
    region_counts: dict[str, int] = {}
    start = time.time()

    for name, lat, lng, radius in REGIONS:
        ac_list = fetch_region(name, lat, lng, radius, session)
        region_counts[name] = len(ac_list)

        for ac in ac_list:
            hex_id = (ac.get('hex') or '').lower()
            if not hex_id:
                continue
            if ac.get('lat') is None or ac.get('lon') is None:
                continue
            # If aircraft seen in multiple regions (overlap), keep first
            if hex_id in registry:
                continue
            registry[hex_id] = {
                'hex':      hex_id,
                'flight':   (ac.get('flight') or '').strip(),
                'r':        ac.get('r') or '',
                't':        ac.get('t') or '',
                'category': ac.get('category') or '',
                'lat':      ac.get('lat'),
                'lon':      ac.get('lon'),
                'alt_baro': ac.get('alt_baro'),
                'alt_geom': ac.get('alt_geom'),
                'gs':       ac.get('gs'),
                'track':    ac.get('track'),
                'squawk':   ac.get('squawk'),
                'emergency': ac.get('emergency') or 'none',
                '_category': classify(ac),
                '_sourceRegion': name,
            }

        time.sleep(INTER_POLL_DELAY)

    elapsed = time.time() - start

    # Summary counts per our category
    cat_counts = {'doi': 0, 'fire': 0, 'military': 0, 'medevac': 0, 'helo': 0, 'civilian': 0}
    for entry in registry.values():
        cat_counts[entry['_category']] = cat_counts.get(entry['_category'], 0) + 1

    output = {
        'generated_at':    datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'schema_version':  1,
        'total':           len(registry),
        'counts':          cat_counts,
        'regions':         region_counts,
        'elapsed_sec':     round(elapsed, 1),
        'primary':         PRIMARY,
        'failover':        FAILOVER,
        'aircraft':        list(registry.values()),
    }

    os.makedirs('data', exist_ok=True)
    out_path = 'data/aircraft.json'
    with open(out_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    size_kb = os.path.getsize(out_path) / 1024
    print()
    print(f'Total aircraft: {len(registry)}')
    print(f'By category: {cat_counts}')
    print(f'Elapsed: {elapsed:.1f}s')
    print(f'Output: {out_path} ({size_kb:.1f} KB)')

    # Also write a slim file with just priority aircraft (fire/mil/medevac/helo)
    # for low-bandwidth operational views and AI context injection.
    priority = [ac for ac in registry.values() if ac['_category'] != 'civilian']
    slim = dict(output)
    slim['aircraft'] = priority
    slim['total'] = len(priority)
    with open('data/aircraft-priority.json', 'w') as f:
        json.dump(slim, f, separators=(',', ':'))
    print(f'Priority-only: data/aircraft-priority.json ({len(priority)} aircraft)')

    return 0


if __name__ == '__main__':
    sys.exit(main())
