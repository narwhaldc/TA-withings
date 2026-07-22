# TA-withings → Splunk — Installation Guide

Setup for the Withings data pipeline into the Wearables platform.
**App version:** TA-withings `0.1.0` · **Ingest:** `tools/withings_to_hec.py` (Withings Measure API → HEC, OAuth2 pull)

---

## Architecture
```
Withings scale → Withings Cloud (Measure/Getmeas API)
                ↓  OAuth2 (scope user.metrics; auto-refresh)
        tools/withings_to_hec.py             (cron; pull)
        decode value*10^unit · compute BMI · per-group dedup · checkpoint · fcntl lock
                ↓  multi-target fan-out (withings_targets.json)
        Splunk HEC → index=wearables, sourcetype=withings:body,
                     indexed fields vendor=withings + person_id
                ↓
        TA-withings (search-time normalization → canonical Wearables model)
        wearables app (data model + dashboards)
```

## Prerequisites
- **`index=wearables`** exists (Settings → Indexes / ACS).
- **`wearables` app** installed (data model + KV registries) and **TA-withings** installed.
- A Splunk **HEC token** with access to `index=wearables`.
- Python 3.9+ on the ingest host (`pip install requests`).
- A **Withings developer app** (client id/secret from developer.withings.com) with an
  OAuth2 redirect URI matching `WITHINGS_REDIRECT_URI` (default `http://localhost:8899/callback`).
  > **Pick the right cloud:** on the developer dashboard choose **Europe Cloud** ("for all
  > developers and partners" = the EU **Public API**, no contract — works for your personal
  > account regardless of where you live). Do **NOT** pick **US Cloud**: that's the contract-only
  > **Medical Cloud** and demands a signed-agreement declaration. Both expose the identical
  > `wbsapi.withings.net` Public API this fetcher uses. Use the **Development** target
  > environment and register `http://localhost:8899/callback` — Withings warns that
  > localhost/http/non-443 callbacks cap the app at **10 users** and can't go "production";
  > that's fine for personal use (you're one user). Ignore the URL **"Test"** button (nothing
  > listens on 8899 until `--auth` runs).

## 1. Install the Splunk apps
Install + restart Splunk: **`TA-withings`** (this add-on) and **`wearables`** (model + dashboards).

## 2. Install the fetcher + deps
The poller is repo-only ingest tooling — **not** in the `.spl`. Copy `tools/withings_to_hec.py`
to your ingest host:
```bash
python3 -m pip install requests
```

## 3. Withings OAuth2 authorization (one-time)
```bash
export WITHINGS_CLIENT_ID='...'         # from developer.withings.com
export WITHINGS_CLIENT_SECRET='...'
python3 withings_to_hec.py --auth        # opens a browser; approve access
```
Tokens persist to `withings_tokens.json` (`WITHINGS_TOKEN_FILE`) and auto-refresh.
> The redirect URI registered in your Withings app **must match** `WITHINGS_REDIRECT_URI`.

## 4. HEC targets (`withings_targets.json`)
Copy the example and fill in your HEC details (same format as `oura_targets.json`):
```bash
cp withings_targets.example.json withings_targets.json
```
```json
{
  "targets": {
    "personal": {
      "hec_url":   "https://splunk:8088/services/collector/event",
      "hec_token": "YOUR-HEC-TOKEN",
      "index":     "wearables",
      "person_id": "P001",
      "verify_ssl": false
    }
  }
}
```
`vendor=withings` + `person_id` are stamped as indexed HEC fields (RBAC key). Sourcetype is
always `withings:body` — do not set a per-target sourcetype. This file holds a token →
**gitignored, never commit.** (Single-target alternative: `SPLUNK_HEC_URL` + `SPLUNK_HEC_TOKEN`
env vars.)

> **Index (recommended: `wearables`).** Set each target's `index` here. If you use a different
> index, it must match the **`widx` macro** in the dashboard apps (see their INSTALLs), so the
> dashboards read the same index your ingest writes to. Change it in both places (target `index`
> + `widx`).

## 5. Populate the registries (admin, KV Store in the wearables app)
```
| makeresults | eval person_id="P001", person_name="Tony", step_goal=10000
| table person_id person_name step_goal | outputlookup wearable_person_profile
```
Device name auto-derives from the Withings model; populate `wearable_device_profile` only to
override with a custom friendly name.

## 6. First run & backfill
```bash
python3 withings_to_hec.py --dry-run                 # print events, send nothing
python3 withings_to_hec.py --backfill 2020-01-01     # history -> HEC
python3 withings_to_hec.py                            # incremental (checkpoint - overlap .. now)
python3 withings_to_hec.py --status                  # per-target coverage
```

## 7. Cron
```cron
30 * * * * cd /opt/withings && /usr/bin/python3 withings_to_hec.py >> withings_to_hec.log 2>&1
```
The `fcntl` lock (`withings_sync.lock`) makes overlapping runs safe; overlap re-fetch dupes are
cleaned by the wearables app's "Wearables Dedup" saved searches (per-group `grpid` dedup also
prevents most re-sends).

## 8. Verify
```
index=wearables vendor=withings | stats count by sourcetype
index=wearables tag=wearable_daily vendor=withings | table _time weight_kg bmi body_fat_pct
```
Then open the wearables dashboards — Withings body-composition values populate the canonical
`weight_kg` / `bmi` / `body_fat_pct` fields (shared with Garmin bodycomp).

## State files (all gitignored — never commit)
`withings_tokens.json` (OAuth tokens) · `withings_targets.json` (HEC tokens) ·
`withings_checkpoint.json` · `withings_dedup_store.json` · `withings_sync.lock`.

## Troubleshooting
- **`No saved tokens`** → run `--auth` first; confirm `WITHINGS_CLIENT_ID/SECRET`.
- **Auth/redirect errors** → the Withings app's registered redirect URI must match `WITHINGS_REDIRECT_URI`.
- **Nothing in Splunk** → check `withings_targets.json` hec_url/token/index; `--dry-run` to inspect payloads.
- **Re-send history** → `--reset-dedup` (all) or `--reset-dedup --target NAME` (one), then `--backfill`.
