# TA-withings → Splunk — Installation Guide

Setup for the Withings data pipeline into the Wearables platform.
**App version:** TA-withings `0.1.9` · **Ingest:** `tools/withings_to_hec.py` (Withings Measure API → HEC, OAuth2 pull)

---

## Architecture
```
Withings scale → Withings Cloud (Measure/Getmeas API)
                ↓  OAuth2 (scope user.metrics + user.activity; auto-refresh)
        tools/withings_to_hec.py             (cron; pull)
        decode value*10^unit · compute BMI · per-group dedup · checkpoint · fcntl lock
                ↓  multi-target fan-out (withings_targets.json)
        Splunk HEC → index=wearables, sourcetype=withings:{body,activity,workouts,sleep},
                     indexed fields vendor=withings + person_id
                ↓
        TA-withings (search-time normalization → canonical Wearables model)
        wearables app (data model + dashboards)
```

## Prerequisites
- **`index=wearables`** exists (Settings → Indexes / ACS).
- **`wearables` app** installed (data model + KV registries) and **TA-withings** installed.
- A Splunk **HEC token** with access to `index=wearables`.
- *(Optional — for log mirroring, §4b)* a second index **`wearables_log`** for ingest logs, and
  the **same HEC token** granted write access to it too (one token, two indexes).
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
The poller is repo-only ingest tooling — **not** in the `.spl` (keeps the app Splunk-Cloud
vetted). Get it onto your ingest host with a one-liner (no git required — just `curl`):
```bash
curl -O https://raw.githubusercontent.com/narwhaldc/TA-withings/main/tools/withings_to_hec.py
python3 -m pip install requests
```
(Or copy it from a checkout / `wget` the same URL.)

## 3. Withings OAuth2 authorization (one-time)
```bash
export WITHINGS_CLIENT_ID='...'         # from developer.withings.com
export WITHINGS_CLIENT_SECRET='...'
python3 withings_to_hec.py --auth        # opens a browser; approve access

> **Upgrading from 0.1.4 or earlier?** 0.1.5 added the activity / sleep / workout
> datasets, which need the **`user.activity`** scope. Re-run `--auth` once to grant it
> (body-composition kept working meanwhile). Same client credentials; the crontab entry is unchanged.
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
set by the fetcher per dataset (withings:body / :activity / :workouts / :sleep) — do not set a per-target sourcetype. This file holds a token →
**gitignored, never commit.** (Single-target alternative: `SPLUNK_HEC_URL` + `SPLUNK_HEC_TOKEN`
env vars.)

> **Index (recommended: `wearables`).** Set each target's `index` here. If you use a different
> index, it must match the **`widx` macro** in the dashboard apps (see their INSTALLs), so the
> dashboards read the same index your ingest writes to. Change it in both places (target `index`
> + `widx`).

## 4b. Optional: mirror ingest logs to Splunk (Ingest Health dashboard)
The fetcher always writes **logfmt** logs to **stderr** (`<ts> level=… comp=withings msg="…" …`) — the
cron redirect (`>> withings_to_hec.log`, below) captures them. To also **mirror those logs into Splunk**
so the wearables **Ingest Health** dashboard can show real success/failure/duration (not just "had
new data"), add a top-level `logging` block to `withings_targets.json`:
```json
"logging": { "method": "hec", "hec_logging_index": "wearables_log" }
```
- With `method: "hec"`, logs go to **each target's own HEC** (reusing that target's `hec_url` +
  `hec_token`) into `hec_logging_index`. Fan out to several Splunks → **each gets its own ingest
  logs** (run-level lines everywhere; per-target lines only to that target's Splunk).
- **Create a second index for the logs** — `wearables_log` — separate from the `wearables` data
  index. Splunk retention is **per-index**, so a separate index lets you keep logs ~30 days while
  health data stays for years.
- **The same HEC token must have write access to BOTH indexes** — the data index (`wearables`) and
  `hec_logging_index` (`wearables_log`). One token, two indexes.
- stderr stays on regardless; **remove the block to log to stderr only.** Logs arrive as sourcetype
  `wearables:ingest`. Endpoint overridable per target with `hec_logging_url` / `hec_logging_token`.
- **Per-person RBAC on the log index:** `person_id` is stamped as an **indexed field** on each per-target log line (`sent events` / `send failed`), and on run-level lines (`run started` / `run complete`) **only when the run is a single person**. So a person-scoped `srchFilter` on `wearables_log` shows a self-manager their own ingest health (including run start/stop/duration), while multi-person aggregator runs keep run-level lines admin-only (the aggregate `events=N` total is not leaked to individuals). To scope logs by person, add `wearables_log` to the wearables role's `srchFilter` (same person_id key as the data index).

## 5. Populate the registries (admin, KV Store in the wearables app)
In the **wearables** app, open **Admin → People & Defaults** and add the person (person_id,
display name, default units, goals/height, and optionally the mapped Splunk login). Blank fields
keep existing values; the page is admin-only (writes are admin/sc_admin-locked). Equivalent raw
SPL if you prefer:
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
