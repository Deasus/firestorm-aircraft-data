#!/usr/bin/env python3
"""
Build a slim FAA Releasable Aircraft Database lookup file.

Source: https://registry.faa.gov/database/ReleasableAircraft.zip
Updated monthly. Contains MASTER.txt (~500K rows) keyed by N-number with
owner name, address, aircraft model, year, airworthiness status.

This script is intentionally NOT auto-run from GHA cron — the source ZIP is
~150MB and the parsed CSV is ~80MB. We'd run it monthly via workflow_dispatch
and commit a slim JSON keyed by N-number → {owner, model, type, year, state}.

Usage:
    python build_faa_index.py               # downloads + processes + writes
    python build_faa_index.py --dry-run     # just verify URL is reachable

Output: data/faa_index.json (~30MB, all 500K records, gzipped to ~10MB)
        data/faa_index_doi_only.json (~50KB, just DOI fleet registrations)

Honest limits:
- Owner names are uppercased and lightly normalized; no fuzzy matching.
- Status field tells us "valid" / "expired" / "deregistered" — useful but
  not real-time (FAA updates monthly).
- Aircraft type codes are FAA codes, not ICAO; we may need to map for display.

Status: SCAFFOLD ONLY (2026-05-05). Not wired into the cron yet. Run manually
once GCP storage or git-lfs is in place — we don't want to commit 30MB of JSON
to a repo every month.
"""

from __future__ import annotations
import argparse
import csv
import io
import json
import os
import sys
import zipfile
from datetime import datetime, timezone

import requests

FAA_RELEASABLE_URL = 'https://registry.faa.gov/database/ReleasableAircraft.zip'
OUT_DIR = os.path.join(os.path.dirname(__file__), 'data')

# Fields we care about from MASTER.txt. The full CSV has ~30 columns; we keep
# a slim subset because the frontend popup needs ~6 fields max.
KEEP_FIELDS = {
    'N-NUMBER':              'reg',
    'NAME':                  'owner',
    'STREET':                None,   # dropped — privacy + payload size
    'STREET2':               None,
    'CITY':                  'city',
    'STATE':                 'state',
    'ZIP CODE':              None,
    'REGION':                'region',
    'COUNTY':                None,
    'COUNTRY':               'country',
    'LAST ACTION DATE':      'last_action',
    'CERT ISSUE DATE':       None,
    'CERTIFICATION':         None,
    'TYPE AIRCRAFT':         'aircraft_type',
    'TYPE ENGINE':           None,
    'STATUS CODE':           'status',
    'MODE S CODE HEX':       'hex',
    'FRACT OWNER':           None,
    'AIR WORTH DATE':        'airworthy_date',
    'OTHER NAMES':           None,
    'EXPIRATION DATE':       None,
    'KIT MFR':               None,
    'KIT MODEL':             None,
}


def fetch_zip(url: str) -> bytes:
    print(f'[FAA] downloading {url}')
    r = requests.get(url, timeout=120, stream=True,
                     headers={'User-Agent':'FIRESTORM-faa-index/1.0'})
    r.raise_for_status()
    chunks = []
    total = 0
    for chunk in r.iter_content(chunk_size=1024*1024):
        if chunk:
            chunks.append(chunk)
            total += len(chunk)
            if total % (10*1024*1024) < len(chunk):
                print(f'  [{total/1024/1024:.0f} MB so far]')
    print(f'[FAA] downloaded {total/1024/1024:.1f} MB')
    return b''.join(chunks)


def parse_master(zip_bytes: bytes) -> list[dict]:
    print('[FAA] extracting MASTER.txt')
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # The file is named MASTER.txt; defensive on case
        name = next((n for n in zf.namelist() if n.upper() == 'MASTER.TXT'), None)
        if not name:
            raise RuntimeError(f'MASTER.txt not in zip; contents: {zf.namelist()}')
        with zf.open(name) as f:
            text = f.read().decode('latin-1')
    rows = []
    reader = csv.DictReader(io.StringIO(text))
    # FAA MASTER.txt has trailing spaces in column headers ("N-NUMBER   ").
    # Build a normalized fieldname map so KEEP_FIELDS lookup works.
    fieldnames = reader.fieldnames or []
    norm_map = {fn.strip().upper(): fn for fn in fieldnames}
    for row in reader:
        slim = {}
        for src_field, dest_field in KEEP_FIELDS.items():
            if dest_field is None:
                continue
            actual_key = norm_map.get(src_field.strip().upper())
            if actual_key is None:
                continue
            v = (row.get(actual_key) or '').strip()
            if v:
                slim[dest_field] = v
        if slim.get('reg'):
            rows.append(slim)
    print(f'[FAA] parsed {len(rows)} valid records')
    return rows


def load_doi_regs() -> set[str]:
    path = os.path.join(OUT_DIR, 'doi_fleet.json')
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        data = json.load(f)
    return {(ac.get('reg') or '').upper().lstrip('N')
            for ac in data.get('aircraft', [])
            if ac.get('reg')}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Only verify the source URL is reachable')
    args = parser.parse_args()

    if args.dry_run:
        r = requests.head(FAA_RELEASABLE_URL, timeout=15,
                          headers={'User-Agent':'FIRESTORM-faa-index/1.0'})
        print(f'[FAA] HEAD {FAA_RELEASABLE_URL} → {r.status_code}')
        print(f'[FAA] Content-Length: {r.headers.get("Content-Length", "?")}')
        return 0

    zip_bytes = fetch_zip(FAA_RELEASABLE_URL)
    rows = parse_master(zip_bytes)

    os.makedirs(OUT_DIR, exist_ok=True)

    # Full index — keyed by N-number for direct lookup. 30MB+. Probably won't
    # commit to repo until we have GCS or git-lfs. For now we write it and
    # let GHA decide whether to commit (default: ignore via .gitignore).
    full_index = {row['reg']: row for row in rows}
    full_path = os.path.join(OUT_DIR, 'faa_index.json')
    with open(full_path, 'w') as f:
        json.dump({
            'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'total': len(full_index),
            'records': full_index,
        }, f, separators=(',', ':'))
    size_mb = os.path.getsize(full_path) / 1024 / 1024
    print(f'[FAA] full index: {full_path} ({size_mb:.1f} MB)')

    # DOI subset — small enough to commit. Fetched on every pipeline run as
    # a sidecar to aircraft.json, so the frontend can show owner info on
    # any DOI aircraft popup.
    doi_regs = load_doi_regs()
    doi_index = {reg: full_index[reg] for reg in doi_regs if reg in full_index}
    doi_path = os.path.join(OUT_DIR, 'faa_index_doi_only.json')
    with open(doi_path, 'w') as f:
        json.dump({
            'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'total': len(doi_index),
            'doi_fleet_size': len(doi_regs),
            'records': doi_index,
        }, f, indent=2)
    print(f'[FAA] DOI subset: {doi_path} ({len(doi_index)}/{len(doi_regs)} regs matched)')

    return 0


if __name__ == '__main__':
    sys.exit(main())
