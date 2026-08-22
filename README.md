# firestorm-aircraft-data

Live ADS-B aircraft feed for [FIRESTORM](https://github.com/Deasus/firestorm-platform).

Polls [airplanes.live](https://airplanes.live) (primary) with [adsb.lol](https://adsb.lol) failover from 21 global regions, merges by ICAO hex, classifies each aircraft into fire/military/medevac/helo/civilian, and publishes a single JSON via GitHub Pages / raw.githubusercontent.com so the FIRESTORM frontend can fetch without getting rate-limited.

## Why this exists

Direct browser polling to airplanes.live hits their rate limit (HTTP 429) quickly when more than a couple of UAT users are active. This pipeline moves the fetch server-side: one cron job per minute, cached JSON on a CDN, any number of downstream viewers.

Same pattern as `firestorm-news-data`, `firestorm-wind-data`, `firestorm-hrrr-data`.

## Cadence

- GitHub Actions cron: every 1 minute (upstream min)
- Per-run wall time: ~32s for 21 regions at 1.5s inter-poll delay
- Output: `data/aircraft.json` (full) + `data/aircraft-priority.json` (fire/mil/medevac/helo only)

## Output schema

```json
{
  "generated_at": "2026-05-04T16:22:00+00:00",
  "schema_version": 1,
  "total": 12847,
  "counts": {"fire": 3, "military": 42, "medevac": 6, "helo": 128, "civilian": 12668},
  "regions": {"CONUS-West": 2134, "CONUS-East": 1891, ...},
  "elapsed_sec": 31.4,
  "primary":  "https://api.airplanes.live/v2/point",
  "failover": "https://api.adsb.lol/v2/point",
  "aircraft": [
    {
      "hex":"aa7e16", "flight":"SWA137", "r":"N7751A", "t":"B737",
      "lat":36.44, "lon":-121.96, "alt_baro":37000,
      "gs":457.4, "track":141.3, "squawk":"3337",
      "emergency":"none", "_category":"civilian",
      "_sourceRegion":"CONUS-West"
    }
  ]
}
```

## Classification rules

- **fire** — callsign prefixes TANKER/T##/CDF/FIRE/CALFIRE/USFS/GUARD/COPTER/BOMBARD/LEAD/ATGS; reg contains USFS/CDF/BLM
- **medevac** — squawk 7500/7600/7700, or callsign LIFE/MED/STAR/MERCY/ANGEL
- **military** — ICAO hex AE*/AF*/ADF*, or callsign RCH/REACH/NAVY/ARMY/PAT/etc.
- **helo** — ADS-B category A7, or type EC35/B407/AS35/H125/S70/etc.
- **civilian** — everything else

## Data source license

- airplanes.live: public feeder-driven community network, no auth required.
- adsb.lol: BSD-3-Clause, CC0 data.

Use responsibly. Don't hammer the APIs.

## Frontend consumption

FIRESTORM fetches:

```
https://raw.githubusercontent.com/Deasus/firestorm-aircraft-data/main/data/aircraft.json
```

Frontend interpolates positions between polls using `lat`+`lon`+`track`+`gs` so motion stays smooth despite 1-min pipeline cadence.
