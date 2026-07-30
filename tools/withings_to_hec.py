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
NOTE: adding the activity/sleep/workout datasets widened the OAuth SCOPE to
include user.activity — existing installs must re-run --auth once to grant it.

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

WBS = "https://wbsapi.withings.net"
AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
TOKEN_URL = WBS + "/v2/oauth2"
MEASURE_URL = WBS + "/measure"
MEASURE_V2_URL = WBS + "/v2/measure"
SLEEP_URL = WBS + "/v2/sleep"
# body measurements are user.metrics; activity / sleep / workouts need user.activity.
# Adding user.activity to an existing grant requires a one-time re-auth (--auth).
SCOPE = "user.metrics,user.activity"

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
MEASTYPES = ",".join(str(t) for t in sorted(TYPE_MAP))

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
                print("[warn] target '%s' has no person_id — required for RBAC" % name)
            targets[name] = {
                "hec_url": c["hec_url"], "hec_token": c["hec_token"],
                "index": c.get("index", "wearables"), "person_id": c.get("person_id"),
                "verify_ssl": c.get("verify_ssl", True),
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
    if args.reset_dedup:
        dedup = load_json(DEDUP_FILE, {})
        if args.target:
            for g in dedup.values():
                g["sent_to"] = [t for t in g.get("sent_to", []) if t != args.target]
            print("removed target '%s' from dedup sent_to lists" % args.target)
        else:
            dedup = {}
            print("cleared dedup store")
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

    # (sourcetype, raw records, decoder, dedup-key builder). Body keeps its bare
    # grpid key for backward-compat with existing dedup stores; new types get a
    # prefix so keys never collide across datasets.
    datasets = [
        ("withings:body", groups, decode_group, lambda ev: str(ev.get("grpid"))),
        ("withings:activity", acts, decode_activity, lambda ev: "act:" + str(ev.get("day"))),
        ("withings:workouts", wks, decode_workout, lambda ev: "wk:" + str(ev.get("workout_id"))),
        ("withings:sleep", slps, decode_sleep, lambda ev: "sl:" + str(ev.get("startdate"))),
    ]

    dedup = load_json(DEDUP_FILE, {})
    sent_total = 0
    for name, tcfg in targets.items():
        for sourcetype, records, decode, keyfn in datasets:
            batch = []
            for raw in records:
                ev = decode(raw)
                key = keyfn(ev)
                rec = dedup.setdefault(key, {"sent_to": []})
                if name in rec["sent_to"]:
                    continue
                batch.append((key, to_hec(tcfg, ev, sourcetype)))
            if not batch:
                continue
            if args.dry_run:
                print("  (dry-run) %s [%s]: %d events" % (name, sourcetype, len(batch)))
                for _, e in batch[:2]:
                    print("    " + json.dumps(e["event"]))
            else:
                hec_send(tcfg, [e for _, e in batch])
                for key, _ in batch:
                    dedup[key]["sent_to"].append(name)
                print("  %s [%s]: sent %d events" % (name, sourcetype, len(batch)))
            sent_total += len(batch)

    if not args.dry_run:
        save_json(DEDUP_FILE, dedup)
        save_json(CHECKPOINT_FILE, {"lastupdate": int(time.time())})
    print("%sdone — %d events across %d target(s)."
          % ("(dry-run) " if args.dry_run else "", sent_total, len(targets)))


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
        sys.exit("another withings_to_hec.py run holds the lock; exiting.")
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

    run_sync(args)


if __name__ == "__main__":
    main()
