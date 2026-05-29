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
    # NA operational priority — 5-region CONUS grid closes the geometry gap
    # that 2 overlapping 500nm disks left in the Dakotas / Montana / Texas /
    # West Texas / SE corridor. N1255A (Air Tractor T-819 over west TX, lat
    # 33 lng -102) was in that gap with the old 2-region layout. Same grid
    # used by the frontend live-polling architecture.
    ('CONUS-West',    38.0, -118.0, 500),
    ('CONUS-Central', 40.0, -100.0, 500),
    ('CONUS-East',    38.0,  -82.0, 500),
    ('CONUS-North',   47.0, -103.0, 500),
    ('CONUS-South',   30.0,  -96.0, 500),
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

# 2026-05-06 — ADSBx Enterprise API now the primary source. Two big wins:
#   1. One /v2/all call returns the global firehose (~14K aircraft) in <2s.
#      Replaces the 24-region sweep. Pipeline cycle drops from ~45s to ~5s.
#   2. ADSBx does NOT honor LADD/PIA filtering. DOI + contracted fleet
#      aircraft become visible in real time — the unique gap that motivated
#      OpenSky-history is largely closed for live data.
# airplanes.live regional sweep is kept as failover. adsb.lol as second-tier.
ADSBX_API_KEY = os.environ.get('ADSBX_API_KEY', '').strip()
ADSBX_ALL_URL = 'https://adsbexchange.com/api/aircraft/v2/all/'
ADSBX_REG_URL = 'https://adsbexchange.com/api/aircraft/v2/registration/{regs}/'

PRIMARY = 'https://api.airplanes.live/v2/point'    # failover only now
FAILOVER = 'https://api.adsb.lol/v2/point'
INTER_POLL_DELAY = 0.5

# 2026-05-29 hardening: ADSBx 402'd today, the legacy regional sweep ran
# without a global cap, and the GHA job timed out (3 min) mid-loop with
# zero output committed. Two layers of defense: tight per-call timeouts so
# any single slow upstream can't burn the budget, and a global deadline so
# we always commit *something* even if upstreams degrade.
ADSBX_TIMEOUT_SEC   = 15
REGION_TIMEOUT_SEC  = 8
SCRIPT_DEADLINE_SEC = 150

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


# ── DOI Contracted Fleet (CWN/EU helicopter contracts) ────────────────
# Separate from DOI_FLEET_REGS (the OAS gov-owned fleet). These are private
# operators on standing DOI contracts that activate during fire ops. Tracked
# as 'contracted' so the operator can filter / ring them distinctly from
# both gov-owned DOI and generic fire-mission aircraft.
def _load_contracted_fleet():
    import os
    path = os.path.join(os.path.dirname(__file__), 'data', 'contracted_fleet.json')
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
        print(f'[CONTRACTED] loaded {len(regs)} contracted-fleet registrations')
        return regs
    except Exception as e:
        print(f'[CONTRACTED] fleet load failed: {e}')
        return set()


CONTRACTED_FLEET_REGS = _load_contracted_fleet()


def classify(ac: dict) -> str:
    """Return one of: 'doi', 'contracted', 'fire', 'medevac', 'military', 'helo', 'civilian'.
    DOI gov-owned is checked FIRST. Contracted comes second — both rank above
    generic 'fire' classification so the operator sees the federal-fleet
    visualization (rings) for these aircraft even when they're flying a
    standard fire mission."""
    hex_id = (ac.get('hex') or '').lower()
    reg = (ac.get('r') or '').upper()
    type_code = (ac.get('t') or '').upper()
    callsign = (ac.get('flight') or '').strip().upper()
    cat = (ac.get('category') or '').upper()
    squawk = (ac.get('squawk') or '').strip()
    # Owner/operator — airplanes.live ships this when known. Catches air tankers
    # that broadcast their raw N-number as callsign (e.g. N1255A = T-819 over
    # west Texas, ownOp = "AIR TRACTOR INC"). Without this we'd miss every
    # privately-owned tanker not flying with a TANKER### callsign.
    own_op = (ac.get('ownOp') or '').upper()

    # DOI Contracted (CWN/EU helicopter operators) — second whitelist
    if reg and reg in CONTRACTED_FLEET_REGS:
        return 'contracted'

    # DOI gov-owned fleet — registration whitelist takes precedence
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
    # Owner/operator-based — air tanker operators that often fly N-number-as-
    # callsign (no TANKER prefix on broadcast). AIR TRACTOR is an SEAT (Single
    # Engine Air Tanker) maker; their AT-802AF is THE workhorse SEAT for fed
    # contracts. NEPTUNE = Neptune Aviation (BAe-146 LATs). TEN TANKER, COULSON,
    # ERICKSON = LAT operators. AERO-FLITE flies CL-415 amphibious. BRIDGER =
    # Bridger Aerospace (CL-415 + Twin Otter). DUNCAN AVIATION services Type-2.
    if own_op:
        for op in ('AIR TRACTOR', 'NEPTUNE', 'COULSON', 'ERICKSON',
                   'BRIDGER', 'TEN TANKER', '10 TANKER', 'AERO-FLITE',
                   'AERO FLITE', 'DAUNTLESS', 'CONAIR'):
            if op in own_op:
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
def fetch_adsbx_all(session: requests.Session) -> list[dict] | None:
    """One-shot global firehose from ADSBx Enterprise. Returns None on any
    failure so the caller can fall back to the regional airplanes.live sweep."""
    if not ADSBX_API_KEY:
        print('[ADSBX] no key — skipping primary, will fall back to airplanes.live')
        return None
    try:
        r = session.get(ADSBX_ALL_URL, timeout=ADSBX_TIMEOUT_SEC,
                        headers={'x-api-key': ADSBX_API_KEY})
        r.raise_for_status()
        data = r.json()
        ac = data.get('ac') or []
        print(f'[ADSBX] /v2/all returned {len(ac)} aircraft')
        return ac
    except Exception as e:
        print(f'[ADSBX] firehose failed: {e}')
        return None


def fetch_adsbx_fleet(session: requests.Session, regs: list[str]) -> list[dict]:
    """Direct registration-batch query for our 79 DOI + 78 contracted tails.
    URLs cap around 8KB; ADSBx accepts comma-separated lists. We chunk at
    80 regs per call to stay well under that."""
    if not ADSBX_API_KEY or not regs:
        return []
    out = []
    for i in range(0, len(regs), 80):
        chunk = regs[i:i+80]
        url = ADSBX_REG_URL.format(regs=','.join(chunk))
        try:
            r = session.get(url, timeout=20,
                            headers={'x-api-key': ADSBX_API_KEY})
            r.raise_for_status()
            ac = (r.json() or {}).get('ac') or []
            out.extend(ac)
        except Exception as e:
            print(f'  [ADSBX fleet chunk {i}] failed: {e}')
    print(f'[ADSBX] fleet batch returned {len(out)} active tails')
    return out


# 2026-05-29: per-endpoint cool-down. If an upstream times out or 5xxs once
# in a run, skip it for the remaining regions instead of burning 8s × N more.
# Reset between runs (module-scope state, fresh per cron invocation).
_dead_endpoints: set[str] = set()

def fetch_region(name: str, lat: float, lng: float, radius: int, session: requests.Session) -> list[dict]:
    """Fetch a single region. Falls back to adsb.lol if airplanes.live returns 429/5xx."""
    for base, label in [(PRIMARY, 'primary'), (FAILOVER, 'failover')]:
        if label in _dead_endpoints:
            continue
        url = f'{base}/{lat}/{lng}/{radius}'
        try:
            r = session.get(url, timeout=REGION_TIMEOUT_SEC)
            if r.status_code == 429:
                print(f'  [{name}] {label} rate limited, marking dead for this run')
                _dead_endpoints.add(label)
                continue
            if r.status_code >= 500:
                print(f'  [{name}] {label} HTTP {r.status_code}, marking dead for this run')
                _dead_endpoints.add(label)
                continue
            r.raise_for_status()
            data = r.json()
            ac = data.get('ac') or []
            print(f'  [{name}] {len(ac)} aircraft (via {label})')
            return ac
        except requests.exceptions.Timeout:
            print(f'  [{name}] {label} timeout, marking dead for this run')
            _dead_endpoints.add(label)
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

    # PRIMARY: ADSBx /v2/all firehose. One call, ~14K aircraft, LADD/PIA-immune.
    adsbx_ac = fetch_adsbx_all(session)
    source_used = 'adsbx' if adsbx_ac is not None else 'airplanes.live'

    if adsbx_ac is not None:
        for ac in adsbx_ac:
            hex_id = (ac.get('hex') or '').lower()
            if not hex_id or ac.get('lat') is None or ac.get('lon') is None:
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
                'ownOp':    ac.get('ownOp') or '',
                'year':     ac.get('year') or '',
                '_category': classify(ac),
                '_sourceRegion': 'ADSBX-global',
            }
        region_counts['ADSBX-global'] = len(registry)

        # SUPPLEMENT: explicit fleet batch — catches any tails the firehose
        # filtering or airbornness may have missed. Same call shape, different
        # endpoint; results merged into the same registry.
        fleet_regs = list(DOI_FLEET_REGS) + list(CONTRACTED_FLEET_REGS)
        fleet_ac = fetch_adsbx_fleet(session, fleet_regs)
        added_from_batch = 0
        for ac in fleet_ac:
            hex_id = (ac.get('hex') or '').lower()
            if not hex_id or ac.get('lat') is None or ac.get('lon') is None:
                continue
            if hex_id in registry:
                continue   # firehose already had it
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
                'ownOp':    ac.get('ownOp') or '',
                'year':     ac.get('year') or '',
                '_category': classify(ac),
                '_sourceRegion': 'ADSBX-fleet',
            }
            added_from_batch += 1
        region_counts['ADSBX-fleet-batch'] = added_from_batch

        # Skip the legacy regional sweep — ADSBx gave us global coverage already.
        elapsed = time.time() - start
        cat_counts = {'doi': 0, 'contracted': 0, 'fire': 0, 'military': 0, 'medevac': 0, 'helo': 0, 'civilian': 0}
        for entry in registry.values():
            cat_counts[entry['_category']] = cat_counts.get(entry['_category'], 0) + 1

        output = {
            'generated_at':    datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'schema_version':  2,
            'source':          source_used,
            'total':           len(registry),
            'counts':          cat_counts,
            'regions':         region_counts,
            'elapsed_sec':     round(elapsed, 1),
            'aircraft':        list(registry.values()),
        }
        os.makedirs('data', exist_ok=True)
        with open('data/aircraft.json', 'w') as f:
            json.dump(output, f, separators=(',', ':'))
        priority = [a for a in registry.values() if a['_category'] != 'civilian']
        with open('data/aircraft-priority.json', 'w') as f:
            json.dump({**output, 'aircraft': priority, 'total': len(priority)}, f, separators=(',', ':'))
        sz = os.path.getsize('data/aircraft.json') / 1024
        sz_p = os.path.getsize('data/aircraft-priority.json') / 1024
        print()
        print(f'Total aircraft: {len(registry)}')
        print(f'By category:    {cat_counts}')
        print(f'Elapsed:        {round(elapsed,1)}s')
        print(f'Output:         data/aircraft.json ({sz:.1f} KB)')
        print(f'Priority-only:  data/aircraft-priority.json ({len(priority)} aircraft, {sz_p:.1f} KB)')
        return 0

    # FAILOVER PATH: legacy 24-region airplanes.live + adsb.lol sweep.
    print('[FAILOVER] ADSBx unavailable, falling back to regional sweep')
    for name, lat, lng, radius in REGIONS:
        # 2026-05-29: watchdog — if both endpoints have died, no point looping;
        # likewise stop if we're past the global deadline. Either case writes
        # what we have so far so the JSON freshness chip stays accurate.
        if {'primary', 'failover'} <= _dead_endpoints:
            print('[WATCHDOG] both upstreams dead, ending sweep early')
            break
        if time.time() - start > SCRIPT_DEADLINE_SEC:
            print(f'[WATCHDOG] over {SCRIPT_DEADLINE_SEC}s, ending sweep early '
                  f'with {len(registry)} aircraft so far')
            break
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
                'ownOp':    ac.get('ownOp') or '',
                'year':     ac.get('year') or '',
                '_category': classify(ac),
                '_sourceRegion': name,
            }

        time.sleep(INTER_POLL_DELAY)

    elapsed = time.time() - start

    # Summary counts per our category
    cat_counts = {'doi': 0, 'contracted': 0, 'fire': 0, 'military': 0, 'medevac': 0, 'helo': 0, 'civilian': 0}
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
