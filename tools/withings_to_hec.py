#!/usr/bin/env python3
"""
withings_to_hec.py — pull Withings data into Splunk HEC.

Mirrors the Oura fetcher's conventions (OAuth2 pull, multi-target fan-out,
per-record dedup, checkpoint, fcntl lock). Sends one flattened event per record
to index=wearables with indexed fields vendor="withings" + person_id; TA-withings
normalizes at search time. Four datasets / sourcetypes:
    withings:body      — body composition (getmeas)          [scope user.metrics]
    withings:activity  — daily activity summary (getactivity) [scope user.activity]
    withings:workouts  — workout sessions (getworkouts)       [scope user.activity]
    withings:sleep     — nightly sleep summary (getsummary)   [scope user.activity]
    withings:device    — device inventory + battery (getdevice) [scope user.info]
    withings:segments  — 6-zone segmental composition (getmeas 173/174/175)
NOTE: the OAuth SCOPE has widened over time (user.activity for activity/sleep/
workouts; user.info for device inventory) — existing installs must re-run --auth
once after an upgrade to grant the newly-added scopes.

REPO-ONLY TOOLING — never shipped in the .spl (it holds credentials).

Setup (one-time):
    export WITHINGS_CLIENT_ID='...'         # from developer.withings.com
    export WITHINGS_CLIENT_SECRET='...'
    python3 withings_to_hec.py --auth        # opens a browser; approve access
Then configure withings_targets.json (see withings_targets.example.json) and run:
    python3 withings_to_hec.py --backfill 2020-01-01   # history
    python3 withings_to_hec.py                          # incremental (cron)
    python3 withings_to_hec.py --status                 # per-target coverage
"""

import argparse
import atexit
import datetime
import fcntl
import http.server
import signal
import json
import os
import sys
import time
import urllib.parse
import webbrowser

import requests

# ---- Splunk-friendly logging (logfmt: <ts> level=.. comp=.. msg=".." key=val) ----
# Duplicated identically across the TA-* print-based fetchers (only _LOG_COMPONENT
# differs); keep in sync. stderr is ALWAYS the source of truth. An optional HEC sink
# (logging.method="hec" in the targets file) mirrors the same lines to Splunk for
# dashboards; it is buffered and flushed at exit (via atexit, so a crash still ships
# the ERROR), and if the flush itself fails (e.g. HEC is the thing that's down) the
# lines are dumped to stderr and NEVER re-sent over HEC. Dry-run never flushes.
_LOG_COMPONENT = "withings"
# Fetcher version — BUMP on every fetcher change (repo-only, not in the .spl);
# emitted as fetcher_ver= on the post-sink "run started" line for drift tracking.
FETCHER_VERSION = "1.1.3"
# Box running this fetcher (its OWN hostname — not Splunk's HEC `host`). Sent as
# run_host= on run-started so Ingest Health shows which box/person to nudge to upgrade.
import socket
RUN_HOST = socket.gethostname()

_LOG_SINKS = []               # [{"url","token","index","verify","targets":set(),"buf":[]}]
_LOG_STATE = {"on": False, "dry": False, "target_pids": {}, "solo_pid": None}


def _logfmt(v):
    s = str(v)
    return '"' + s.replace('"', "'") + '"' if (s == "" or " " in s or "=" in s) else s


def _log(level, msg, **kv):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    extra = "".join(" %s=%s" % (k, _logfmt(v)) for k, v in kv.items())
    line = "%s level=%s comp=%s msg=%s%s" % (ts, level, _LOG_COMPONENT, _logfmt(msg), extra)
    print(line, file=sys.stderr)
    if _LOG_STATE["on"]:
        tgt = kv.get("target")           # scoped: a targeted line goes only to that sink
        # Indexed person_id for RBAC: explicit kwarg, else the target's person_id, else the
        # run's solo person_id (None on multi-person runs → run-level lines stay admin-only).
        pid = kv.get("person_id") or _LOG_STATE["target_pids"].get(tgt) or _LOG_STATE["solo_pid"]
        for sink in _LOG_SINKS:
            if tgt is None or tgt in sink["targets"]:
                sink["buf"].append((time.time(), line, pid))


def log_info(msg, **kv):  _log("INFO", msg, **kv)
def log_warn(msg, **kv):  _log("WARN", msg, **kv)
def log_error(msg, **kv): _log("ERROR", msg, **kv)


def configure_hec_log(global_cfg, targets, dry_run):
    """Set up optional per-target HEC log mirrors. `logging` config may live globally
    (top-level `logging` block) and/or per-target (a `logging` block inside a target;
    the target's block overrides the global). The HEC endpoint/index default to each
    target's OWN hec_url/hec_token/index, so a single global {"method":"hec"} fans logs
    to EVERY target's Splunk. Each mirror gets the run-level lines plus its own target's
    sent/error lines. stderr is unaffected (always on)."""
    _LOG_SINKS.clear()
    _LOG_STATE["dry"] = dry_run
    # person_id map for indexed RBAC on the log events: each target's pid, plus the
    # run's "solo" pid (set only when the whole run is one person — see _log).
    _LOG_STATE["target_pids"] = {tn: tc.get("person_id") for tn, tc in (targets or {}).items()}
    _pids = sorted({p for p in _LOG_STATE["target_pids"].values() if p})
    _LOG_STATE["solo_pid"] = _pids[0] if len(_pids) == 1 else None
    by_key = {}
    for tname, tcfg in (targets or {}).items():
        merged = dict(global_cfg or {})
        merged.update(tcfg.get("logging") or {})
        method = merged.get("method")
        methods = method if isinstance(method, list) else ([method] if method else [])
        if "hec" not in [str(m).lower() for m in methods]:
            continue
        url = merged.get("hec_logging_url") or tcfg.get("hec_url")
        token = merged.get("hec_logging_token") or tcfg.get("hec_token")
        index = merged.get("hec_logging_index") or tcfg.get("index") or "wearables"
        if not (url and token):
            log_warn("hec log sink skipped: no hec_url/hec_token", target=tname)
            continue
        verify = merged.get("verify_ssl", tcfg.get("verify_ssl", True))
        sink = by_key.get((url, token, index))
        if sink is None:
            sink = {"url": url, "token": token, "index": index, "verify": verify,
                    "targets": set(), "buf": []}
            by_key[(url, token, index)] = sink
            _LOG_SINKS.append(sink)
        sink["targets"].add(tname)
    if _LOG_SINKS:
        _LOG_STATE["on"] = True
        atexit.register(flush_hec_log)
        log_info("hec log sink enabled", sinks=len(_LOG_SINKS),
                 hec_index=",".join(sorted({s["index"] for s in _LOG_SINKS})))


def flush_hec_log():
    """POST each sink's buffered log lines as raw logfmt events. Best-effort: a failure
    NEVER re-sends over HEC and NEVER fails the run — it dumps to stderr. Dry-run: skip."""
    for sink in _LOG_SINKS:
        buf = sink["buf"]
        sink["buf"] = []
        if not buf or _LOG_STATE["dry"]:
            continue
        events = []
        for t, line, pid in buf:
            ev = {"time": t, "event": line, "sourcetype": "wearables:ingest", "index": sink["index"]}
            if pid:
                ev["fields"] = {"person_id": pid}   # indexed field → RBAC scoping on the log index
            events.append(json.dumps(ev))
        body = "".join(events)
        try:
            verify = sink["verify"] if str(sink["url"]).startswith("https") else False
            r = requests.post(sink["url"], data=body,
                              headers={"Authorization": "Splunk " + sink["token"]},
                              verify=verify, timeout=30)
            r.raise_for_status()
        except Exception as e:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print('%s level=WARN comp=%s msg="hec log flush failed" error=%s target=%s count=%d'
                  % (ts, _LOG_COMPONENT, type(e).__name__,
                     ",".join(sorted(sink["targets"])), len(buf)), file=sys.stderr)


def load_logging_cfg():
    """Top-level `logging` block from the targets file (or {} if none/absent)."""
    try:
        return (json.loads(open(str(TARGETS_FILE)).read()) or {}).get("logging") or {}
    except Exception:
        return {}

WBS = "https://wbsapi.withings.net"
AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
TOKEN_URL = WBS + "/v2/oauth2"
MEASURE_URL = WBS + "/measure"
MEASURE_V2_URL = WBS + "/v2/measure"
SLEEP_URL = WBS + "/v2/sleep"
USER_V2_URL = WBS + "/v2/user"          # getdevice (device inventory + battery)
_RUN_EPOCH = int(time.time())           # observation time stamped on device-status events
# body measurements are user.metrics; activity / sleep / workouts need user.activity;
# device inventory (getdevice: battery, model, last-sync) needs user.info.
# Widening the scope requires a one-time re-auth (--auth) on existing installs.
SCOPE = "user.metrics,user.activity,user.info"

HERE = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.getenv("WITHINGS_TOKEN_FILE", os.path.join(HERE, "withings_tokens.json"))
TARGETS_FILE = os.path.join(HERE, "withings_targets.json")
CHECKPOINT_FILE = os.path.join(HERE, "withings_checkpoint.json")
DEDUP_FILE = os.path.join(HERE, "withings_dedup_store.json")
LOCK_FILE = os.path.join(HERE, "withings_sync.lock")
REDIRECT_URI = os.getenv("WITHINGS_REDIRECT_URI", "http://localhost:8899/callback")
OVERLAP_DAYS = int(os.getenv("WITHINGS_CHECKPOINT_OVERLAP_DAYS", "2"))

# Withings measure type -> decoded field name (real_value = value * 10^unit)
TYPE_MAP = {
    1: "weight", 4: "height", 5: "fat_free_mass", 6: "fat_ratio", 8: "fat_mass",
    9: "diastolic_bp", 10: "systolic_bp", 11: "heart_pulse", 12: "temperature",
    54: "spo2", 71: "body_temp", 73: "skin_temp", 76: "muscle_mass",
    77: "hydration", 88: "bone_mass", 91: "pulse_wave_velocity", 123: "vo2max",
    170: "visceral_fat",
}
# Segmental (6-zone) composition — Withings confirmed 2026-08-18 (Haley English):
# 173 = fat-free mass / segment, 174 = fat mass / segment, 175 = muscle mass / segment.
# These return MULTIPLE measures per type (one per body zone), so they can't share the
# scalar TYPE_MAP (which keeps one value per type). We REQUEST them and, until the raw
# per-segment structure is confirmed, emit the raw measures for inspection rather than
# decode blindly. See TA-withings-todo / wearables #56.
SEGMENTAL_TYPES = {173, 174, 175}   # per-zone: 173 fat-free, 174 fat, 175 muscle
MEASTYPES = ",".join(str(t) for t in sorted(set(TYPE_MAP) | SEGMENTAL_TYPES))

# ---- Part B: activity / sleep / workout endpoints (require user.activity scope) ----
ACTIVITY_FIELDS = ("steps,distance,elevation,soft,moderate,intense,active,calories,"
                   "totalcalories,hr_average,hr_min,hr_max,hr_zone_0,hr_zone_1,hr_zone_2,hr_zone_3")
WORKOUT_FIELDS = ("calories,intensity,manual_distance,manual_calories,hr_average,hr_min,hr_max,"
                  "hr_zone_0,hr_zone_1,hr_zone_2,hr_zone_3,steps,distance,elevation,pool_length,pool_laps")
SLEEP_FIELDS = ("total_sleep_time,total_timeinbed,sleep_score,asleepduration,deepsleepduration,"
                "lightsleepduration,remsleepduration,wakeupduration,durationtosleep,durationtowakeup,"
                "wakeupcount,hr_average,hr_min,hr_max,rr_average,rr_min,rr_max,"
                "breathing_disturbances_intensity,snoring,snoringepisodecount,sleep_efficiency,"
                "sleep_latency,apnea_hypopnea_index,nb_rem_episodes,out_of_bed_count")

# Withings numeric workout category -> label. Labels feed the wearables activity
# taxonomy lookup (case-insensitive) -> workout_activity_canon; unknown -> "Other".
WORKOUT_CATEGORY = {
    1: "Walking", 2: "Running", 3: "Hiking", 4: "Skating", 6: "Cycling", 7: "Swimming",
    8: "Surfing", 10: "Windsurfing", 12: "Tennis", 13: "Table tennis", 14: "Squash",
    15: "Badminton", 16: "Strength training", 17: "Calisthenics", 18: "Elliptical",
    19: "Pilates", 20: "Basketball", 21: "Soccer", 22: "Football", 23: "Rugby",
    24: "Volleyball", 26: "Horse riding", 27: "Golf", 28: "Yoga", 29: "Dancing",
    30: "Boxing", 31: "Fencing", 32: "Wrestling", 33: "Martial arts", 34: "Skiing",
    35: "Snowboarding", 36: "Other", 187: "Rowing", 188: "Zumba", 191: "Baseball",
    192: "Handball", 193: "Hockey", 194: "Ice hockey", 195: "Climbing",
    196: "Ice skating", 272: "Multi-sport", 306: "Walking", 307: "Meditation",
}


# ---------------------------------------------------------------- small helpers
def load_dotenv():
    """Populate os.environ from a local .env (KEY=VALUE lines) next to this script,
    if present. Existing environment values win; a leading 'export ' and surrounding
    quotes are stripped. .env is gitignored (it holds credentials) — never commit it."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):]
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except IOError:
        pass


load_dotenv()  # pick up creds/config from .env next to this script (gitignored)


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (IOError, ValueError):
        return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def client_creds():
    cid = os.getenv("WITHINGS_CLIENT_ID")
    secret = os.getenv("WITHINGS_CLIENT_SECRET")
    if not cid or not secret:
        sys.exit("Set WITHINGS_CLIENT_ID and WITHINGS_CLIENT_SECRET (from developer.withings.com).")
    return cid, secret


def wbs_call(url, data, bearer=None):
    """POST to the Withings API; unwrap the {status, body} envelope."""
    headers = {"Authorization": "Bearer " + bearer} if bearer else {}
    r = requests.post(url, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if payload.get("status") != 0:
        raise RuntimeError("Withings API error status=%s: %s"
                           % (payload.get("status"), payload.get("error", payload)))
    return payload.get("body", {})


# ------------------------------------------------------------------ OAuth2 flow
class _CodeHandler(http.server.BaseHTTPRequestHandler):
    code = None

    def do_GET(self):
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        _CodeHandler.code = (params.get("code") or [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        msg = b"<h2>Withings authorization received. You can close this tab.</h2>"
        self.wfile.write(msg)

    def log_message(self, *a):  # silence
        pass


def do_auth():
    cid, secret = client_creds()
    parsed = urllib.parse.urlparse(REDIRECT_URI)
    port = parsed.port or 8899
    state = "wearables-%d" % int(time.time())
    q = urllib.parse.urlencode({
        "response_type": "code", "client_id": cid, "scope": SCOPE,
        "redirect_uri": REDIRECT_URI, "state": state,
    })
    url = AUTHORIZE_URL + "?" + q
    print("Opening browser to authorize Withings access:\n  " + url)
    server = http.server.HTTPServer(("localhost", port), _CodeHandler)
    webbrowser.open(url)
    server.handle_request()  # blocks until the redirect hits /callback
    code = _CodeHandler.code
    if not code:
        sys.exit("No authorization code received.")
    body = wbs_call(TOKEN_URL, {
        "action": "requesttoken", "grant_type": "authorization_code",
        "client_id": cid, "client_secret": secret, "code": code,
        "redirect_uri": REDIRECT_URI,
    })
    body["obtained_at"] = int(time.time())
    save_json(TOKEN_FILE, body)
    print("Authorized. Tokens saved to %s (userid=%s)." % (TOKEN_FILE, body.get("userid")))


def access_token():
    """Return a valid access token, refreshing if near expiry."""
    tok = load_json(TOKEN_FILE, None)
    if not tok:
        sys.exit("No saved tokens — run:  python3 withings_to_hec.py --auth")
    age = int(time.time()) - tok.get("obtained_at", 0)
    if age >= tok.get("expires_in", 10800) - 300:  # refresh 5 min early
        cid, secret = client_creds()
        tok = wbs_call(TOKEN_URL, {
            "action": "requesttoken", "grant_type": "refresh_token",
            "client_id": cid, "client_secret": secret,
            "refresh_token": tok["refresh_token"],
        })
        tok["obtained_at"] = int(time.time())
        save_json(TOKEN_FILE, tok)
    return tok["access_token"]


# --------------------------------------------------------------------- targets
def load_targets(target_filter=None):
    cfg = load_json(TARGETS_FILE, None)
    targets = {}
    if cfg and cfg.get("targets"):
        for name, c in cfg["targets"].items():
            if not c.get("person_id"):
                log_warn("target missing person_id (required for RBAC)", target=name)
            targets[name] = {
                "hec_url": c["hec_url"], "hec_token": c["hec_token"],
                "index": c.get("index", "wearables"), "person_id": c.get("person_id"),
                "verify_ssl": c.get("verify_ssl", True), "logging": c.get("logging"),
            }
    elif os.getenv("SPLUNK_HEC_URL") and os.getenv("SPLUNK_HEC_TOKEN"):
        targets["default"] = {
            "hec_url": os.environ["SPLUNK_HEC_URL"], "hec_token": os.environ["SPLUNK_HEC_TOKEN"],
            "index": os.getenv("SPLUNK_INDEX", "wearables"),
            "person_id": os.getenv("WITHINGS_PERSON_ID", "P001"),
            "verify_ssl": os.getenv("SPLUNK_VERIFY_SSL", "true").lower() == "true",
        }
    else:
        sys.exit("No targets: create withings_targets.json or set SPLUNK_HEC_URL/TOKEN.")
    if target_filter:
        if target_filter not in targets:
            sys.exit("target '%s' not found. have: %s" % (target_filter, list(targets)))
        targets = {target_filter: targets[target_filter]}
    return targets


# ----------------------------------------------------------------- getmeas pull
def getmeas(token, lastupdate=None, startdate=None, enddate=None):
    """Return all measuregrps (handles pagination via more/offset)."""
    groups, offset = [], 0
    while True:
        data = {"action": "getmeas", "meastypes": MEASTYPES, "category": 1, "offset": offset}
        if lastupdate is not None:
            data["lastupdate"] = int(lastupdate)
        else:
            data["startdate"] = int(startdate)
            data["enddate"] = int(enddate)
        body = wbs_call(MEASURE_URL, data, bearer=token)
        groups.extend(body.get("measuregrps", []))
        if body.get("more"):
            offset = body.get("offset", 0)
        else:
            break
    return groups


def decode_group(grp):
    """Flatten one measuregrp into a named-field event (real values)."""
    ev = {"grpid": grp.get("grpid"), "deviceid": grp.get("deviceid"),
          "modelid": grp.get("modelid"), "_epoch": grp.get("date")}
    ev["day"] = datetime.datetime.utcfromtimestamp(grp.get("date", 0)).strftime("%Y-%m-%d")
    for m in grp.get("measures", []):
        name = TYPE_MAP.get(m.get("type"))
        if name:
            ev[name] = round(m["value"] * (10 ** m["unit"]), 3)
    if ev.get("weight") and ev.get("height"):
        ev["bmi"] = round(ev["weight"] / (ev["height"] ** 2), 1)
    return ev


# --- Segmental (6-zone) composition -> withings:segments (one event per zone) --------
# Structure confirmed 2026-08-18 (Haley English + raw sample): each segmental measure
# carries a `position` tagging its body zone, and the 5 zones partition the whole body
# (their sum == the whole-body value). value_kg = value * 10^unit.
#   types:     173 = fat-free mass, 174 = fat mass, 175 = muscle mass
#   positions: 12 = trunk, 2/3 = arms, 10/11 = legs   (L/R tentative — confirm w/ Haley)
SEGMENT_POSITION = {12: "trunk", 2: "arm_right", 3: "arm_left",
                    10: "leg_right", 11: "leg_left"}
_SEG_TYPE_FIELD = {173: "fat_free_mass_kg", 174: "fat_mass_kg", 175: "muscle_mass_kg"}


def extract_segments(groups):
    """Expand groups carrying segmental measures into one flat record per body zone."""
    out = []
    for grp in groups:
        by_pos = {}
        for m in grp.get("measures", []):
            fld = _SEG_TYPE_FIELD.get(m.get("type"))
            if fld is not None:
                by_pos.setdefault(m.get("position"), {})[fld] = round(m["value"] * (10 ** m["unit"]), 3)
        for pos, vals in by_pos.items():
            rec = {"grpid": grp.get("grpid"), "deviceid": grp.get("deviceid"),
                   "modelid": grp.get("modelid"), "_epoch": grp.get("date"),
                   "day": datetime.datetime.utcfromtimestamp(grp.get("date", 0)).strftime("%Y-%m-%d"),
                   "position": pos, "segment": SEGMENT_POSITION.get(pos, "pos_" + str(pos))}
            rec.update(vals)
            out.append(rec)
    return out


def decode_segment(rec):
    """Segment records from extract_segments are already flat named-field events."""
    return rec


# ----------------------------------------------- activity / sleep / workout pulls
def _paged(url, action, extra, token, lastupdate, startymd, endymd, result_key):
    """Generic v2 paginated pull for getactivity / getworkouts / getsummary.
    Incremental uses lastupdate (epoch); backfill uses startdateymd/enddateymd."""
    base = {"action": action}
    base.update(extra)
    if lastupdate is not None:
        base["lastupdate"] = int(lastupdate)
    else:
        base["startdateymd"] = startymd
        base["enddateymd"] = endymd
    out, offset = [], 0
    while True:
        data = dict(base)
        data["offset"] = offset
        body = wbs_call(url, data, bearer=token)
        out.extend(body.get(result_key, []))
        if body.get("more"):
            offset = body.get("offset", 0)
        else:
            break
    return out


def get_activity(token, lastupdate=None, startymd=None, endymd=None):
    return _paged(MEASURE_V2_URL, "getactivity", {"data_fields": ACTIVITY_FIELDS},
                  token, lastupdate, startymd, endymd, "activities")


def get_workouts(token, lastupdate=None, startymd=None, endymd=None):
    return _paged(MEASURE_V2_URL, "getworkouts", {"data_fields": WORKOUT_FIELDS},
                  token, lastupdate, startymd, endymd, "series")


def get_sleep(token, lastupdate=None, startymd=None, endymd=None):
    return _paged(SLEEP_URL, "getsummary", {"data_fields": SLEEP_FIELDS},
                  token, lastupdate, startymd, endymd, "series")


def get_devices(token):
    """Device inventory + status (getdevice, needs user.info scope). Not time-ranged
    — returns the current snapshot. Fails soft: if the scope isn't granted yet, log a
    warning and skip so the rest of the run still succeeds."""
    try:
        body = wbs_call(USER_V2_URL, {"action": "getdevice"}, bearer=token)
        return body.get("devices", [])
    except Exception as e:
        log_warn("getdevice skipped (needs user.info scope? re-run --auth to grant)",
                 error=type(e).__name__)
        return []


def decode_device(dev):
    """One event per Withings device: model + battery (high/medium/low) + last-sync.
    Stamped at observation time (_RUN_EPOCH) so battery/sync form a time series."""
    return {
        "deviceid": dev.get("deviceid"),
        "device_type": dev.get("type"),
        "model": dev.get("model"),
        "model_id": dev.get("model_id"),
        "battery": dev.get("battery"),                 # high / medium / low
        "last_session_date": dev.get("last_session_date"),
        "first_session_date": dev.get("first_session_date"),
        "timezone": dev.get("timezone"),
        "_epoch": _RUN_EPOCH,
        "day": datetime.datetime.utcfromtimestamp(_RUN_EPOCH).strftime("%Y-%m-%d"),
    }


def _ymd_epoch(ymd):
    return int(datetime.datetime.strptime(ymd, "%Y-%m-%d")
               .replace(tzinfo=datetime.timezone.utc).timestamp())


def decode_activity(a):
    """getactivity: metric fields are top-level per day (no 'data' wrapper)."""
    drop = {"timezone", "deviceid", "hash_deviceid", "brand", "is_tracker"}
    ev = {k: v for k, v in a.items() if k not in drop}
    ev["day"] = a.get("date")
    ev["_epoch"] = _ymd_epoch(a["date"]) if a.get("date") else None
    return ev


def decode_workout(w):
    """getworkouts: metrics nested under 'data'; category is a numeric type."""
    ev = dict(w.get("data", {}))
    cat = w.get("category")
    ev["category"] = cat
    ev["activity"] = WORKOUT_CATEGORY.get(cat, "Other")
    ev["workout_id"] = w.get("id")
    ev["startdate"] = w.get("startdate")
    ev["enddate"] = w.get("enddate")
    ev["day"] = w.get("date")
    ev["_epoch"] = w.get("startdate")
    return ev


def decode_sleep(s):
    """sleep getsummary: metrics nested under 'data'; one summary per night."""
    ev = dict(s.get("data", {}))
    ev["startdate"] = s.get("startdate")
    ev["enddate"] = s.get("enddate")
    ev["day"] = s.get("date")
    ev["_epoch"] = s.get("startdate")
    return ev


# --------------------------------------------------------------------- HEC send
def to_hec(target, ev, sourcetype="withings:body"):
    e = dict(ev)
    epoch = e.pop("_epoch", None) or time.time()
    return {"time": epoch, "sourcetype": sourcetype, "index": target["index"],
            "event": e,
            "fields": {"vendor": "withings", "person_id": target["person_id"]}}


def hec_send(target, batch):
    url = target["hec_url"] + ("/services/collector/event"
                               if not target["hec_url"].rstrip("/").endswith("event") else "")
    verify = target.get("verify_ssl", True) if target["hec_url"].startswith("https") else False
    payload = "".join(json.dumps(e) for e in batch)
    r = requests.post(target["hec_url"], data=payload,
                      headers={"Authorization": "Splunk " + target["hec_token"]},
                      verify=verify, timeout=30)
    r.raise_for_status()


# -------------------------------------------------------------------- main sync
def run_sync(args):
    targets = load_targets(args.target)
    configure_hec_log(load_logging_cfg(), targets, args.dry_run)
    if args.reset_dedup:
        dedup = load_json(DEDUP_FILE, {})
        if args.target:
            for g in dedup.values():
                g["sent_to"] = [t for t in g.get("sent_to", []) if t != args.target]
            log_info("removed target from dedup", target=args.target)
        else:
            dedup = {}
            log_info("cleared dedup store")
        save_json(DEDUP_FILE, dedup)
        return

    if args.status:
        cp = load_json(CHECKPOINT_FILE, {})
        dedup = load_json(DEDUP_FILE, {})
        print("checkpoint lastupdate: %s" % cp.get("lastupdate"))
        print("known measurement groups: %d" % len(dedup))
        for name in targets:
            n = sum(1 for g in dedup.values() if name in g.get("sent_to", []))
            print("  %s: %d groups sent" % (name, n))
        return

    token = access_token()
    t0 = time.time()
    log_info("run started", fetcher_ver=FETCHER_VERSION, run_host=RUN_HOST, mode=("backfill" if args.backfill else "incremental"), targets=len(targets))
    now = int(time.time())
    today_ymd = datetime.datetime.utcfromtimestamp(now).strftime("%Y-%m-%d")
    if args.backfill:
        start = int(datetime.datetime.strptime(args.backfill, "%Y-%m-%d").timestamp())
        groups = getmeas(token, startdate=start, enddate=now)
        acts = get_activity(token, startymd=args.backfill, endymd=today_ymd)
        wks = get_workouts(token, startymd=args.backfill, endymd=today_ymd)
        slps = get_sleep(token, startymd=args.backfill, endymd=today_ymd)
    else:
        cp = load_json(CHECKPOINT_FILE, {})
        last = cp.get("lastupdate")
        if last:
            last = int(last) - OVERLAP_DAYS * 86400
        else:
            last = now - 3650 * 86400  # first run: ~10y back
        groups = getmeas(token, lastupdate=last)
        acts = get_activity(token, lastupdate=last)
        wks = get_workouts(token, lastupdate=last)
        slps = get_sleep(token, lastupdate=last)

    devices = get_devices(token)   # device inventory + battery (current snapshot, both modes)

    # (sourcetype, raw records, decoder, dedup-key builder). Body keeps its bare
    # grpid key for backward-compat with existing dedup stores; new types get a
    # prefix so keys never collide across datasets.
    datasets = [
        ("withings:body", groups, decode_group, lambda ev: str(ev.get("grpid"))),
        ("withings:segments", extract_segments(groups), decode_segment,
         lambda ev: "seg:%s:%s" % (ev.get("grpid"), ev.get("position"))),
        ("withings:activity", acts, decode_activity, lambda ev: "act:" + str(ev.get("day"))),
        ("withings:workouts", wks, decode_workout, lambda ev: "wk:" + str(ev.get("workout_id"))),
        ("withings:sleep", slps, decode_sleep, lambda ev: "sl:" + str(ev.get("startdate"))),
        ("withings:device", devices, decode_device,
         lambda ev: "dev:%s:%s:%s" % (ev.get("deviceid"), ev.get("battery"), ev.get("last_session_date"))),
    ]

    dedup = load_json(DEDUP_FILE, {})
    sent_total = 0
    skipped_total = 0
    for name, tcfg in targets.items():
        for sourcetype, records, decode, keyfn in datasets:
            batch = []
            skipped = 0
            for raw in records:
                ev = decode(raw)
                key = keyfn(ev)
                rec = dedup.setdefault(key, {"sent_to": []})
                if name in rec["sent_to"]:
                    skipped += 1
                    continue
                batch.append((key, to_hec(tcfg, ev, sourcetype)))
            skipped_total += skipped
            if not batch:
                continue
            if args.dry_run:
                log_info("dry-run batch", person_id=tcfg.get("person_id"), target=name, sourcetype=sourcetype, count=len(batch), skipped=skipped)
                for _, e in batch[:2]:
                    print("    " + json.dumps(e["event"]))
            else:
                hec_send(tcfg, [e for _, e in batch])
                for key, _ in batch:
                    dedup[key]["sent_to"].append(name)
                log_info("sent events", person_id=tcfg.get("person_id"), target=name, sourcetype=sourcetype, count=len(batch), skipped=skipped)
            sent_total += len(batch)

    if not args.dry_run:
        save_json(DEDUP_FILE, dedup)
        save_json(CHECKPOINT_FILE, {"lastupdate": int(time.time())})
    log_info("run complete", events=sent_total, skipped=skipped_total, targets=len(targets),
             duration_s=round(time.time() - t0, 1), dry_run=args.dry_run)


def main():
    ap = argparse.ArgumentParser(description="Withings -> Splunk HEC (body composition).")
    ap.add_argument("--auth", action="store_true", help="run the one-time OAuth2 browser flow")
    ap.add_argument("--backfill", metavar="YYYY-MM-DD", help="pull history from this date")
    ap.add_argument("--dry-run", action="store_true", help="print events, send nothing")
    ap.add_argument("--status", action="store_true", help="checkpoint + per-target coverage")
    ap.add_argument("--reset-dedup", action="store_true", help="clear dedup (all, or one --target)")
    ap.add_argument("--target", metavar="NAME", help="limit to a single named target")
    args = ap.parse_args()

    if args.auth:
        do_auth()
        return

    # exclusive lock: flock auto-releases on exit/crash; we also remove the lock
    # FILE on clean exit (atexit + SIGTERM/SIGINT) so no stale file lingers.
    # Registered only AFTER we hold the lock, so a losing instance can't delete it.
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        log_warn("another run holds the lock; exiting")
        sys.exit(1)
    lock.write(str(os.getpid()))
    lock.flush()

    def _release_lock():
        try:
            fcntl.flock(lock, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            lock.close()
        except Exception:
            pass
        try:
            os.unlink(LOCK_FILE)
        except OSError:
            pass
    atexit.register(_release_lock)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))

    try:
        run_sync(args)
    except Exception as e:
        log_error("run failed", error=type(e).__name__, detail=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
