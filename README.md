# TA-withings — Withings Add-on for the Wearables platform

Normalizes Withings body-composition data (weight, BMI, body-fat %) into the
canonical **Wearables** data model, so it lights up the same body-composition
panels the platform already renders for other vendors — no data-model changes.

- **App (`.spl`):** search-time normalization only — `props.conf` / `eventtypes.conf`
  / `tags.conf`. Maps `sourcetype=withings:body` → canonical `weight_kg`, `bmi`,
  `body_fat_pct` on the Daily model root; tags `wearable_daily`.
- **Ingest (`tools/`, repo-only):** `withings_to_hec.py` — an OAuth2 pull poller
  (Withings Measure *Getmeas* API) that flattens Withings' `value/type/unit`
  measure arrays into named fields and sends them to Splunk HEC
  (`index=wearables`, indexed `vendor=withings` + `person_id`). **Never shipped
  in the `.spl`** — it holds credentials.

## Architecture
```
Withings scale → Withings Cloud (Measure API)
                     ↓  OAuth2 (scope user.metrics; auto-refresh)
        tools/withings_to_hec.py         (cron; pull)
        decode value*10^unit · compute BMI · per-group dedup · checkpoint · lock
                     ↓  multi-target fan-out (withings_targets.json)
        Splunk HEC → index=wearables, sourcetype=withings:body,
                     indexed fields vendor=withings + person_id
                     ↓
        TA-withings (search-time normalization → canonical Wearables model)
        wearables app (data model + dashboards, shared with Oura/Garmin)
```

## Measure mapping
The poller decodes Withings measure types (real value = `value × 10^unit`):
weight (1) → `weight_kg`, fat ratio (6) → `body_fat_pct`, height (4) + weight →
computed `bmi`. It also carries fat mass (8), fat-free mass (5), muscle mass (76),
bone mass (88), hydration (77), and — if present — heart pulse (11), SpO2 (54),
blood pressure (9/10) as raw fields (not yet modeled).

See **INSTALL.md** for setup. Part of the multi-vendor **wearables** platform
(with **TA-oura** and **TA-garmin**). Source: https://github.com/narwhaldc
