#!/usr/bin/env python3
"""
withings_to_hec.py — pull Withings body-composition measurements into Splunk HEC.

Mirrors the Oura fetcher's conventions (OAuth2 pull, multi-target fan-out,
per-record dedup, checkpoint, fcntl lock). Sends one flattened event per Withings
measurement group to index=wearables, sourcetype=withings:body, with indexed
fields vendor="withings" + person_id. TA-withings normalizes those at search time.

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
import datetime
import fcntl
import http.server
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
SCOPE = "user.metrics"

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
}
MEASTYPES = ",".join(str(t) for t in sorted(TYPE_MAP))


# ---------------------------------------------------------------- small helpers
def load_dotenv(path=os.path.join(HERE, ".env")):
    """Populate os.environ from a local .env (KEY=VALUE lines) if present.
    Existing environment values win; 'export ' prefix and surrounding quotes are
    stripped. .env is gitignored (holds credentials) — never commit it."""
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
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except IOError:
        pass


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


# --------------------------------------------------------------------- HEC send
def to_hec(target, ev):
    e = dict(ev)
    epoch = e.pop("_epoch", None) or time.time()
    return {"time": epoch, "sourcetype": "withings:body", "index": target["index"],
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
    if args.backfill:
        start = int(datetime.datetime.strptime(args.backfill, "%Y-%m-%d").timestamp())
        groups = getmeas(token, startdate=start, enddate=int(time.time()))
    else:
        cp = load_json(CHECKPOINT_FILE, {})
        last = cp.get("lastupdate")
        if last:
            last = int(last) - OVERLAP_DAYS * 86400
        else:
            last = int(time.time()) - 3650 * 86400  # first run: ~10y back
        groups = getmeas(token, lastupdate=last)

    dedup = load_json(DEDUP_FILE, {})
    sent_total = 0
    for name, tcfg in targets.items():
        batch = []
        for grp in groups:
            ev = decode_group(grp)
            key = str(ev["grpid"])
            rec = dedup.setdefault(key, {"sent_to": []})
            if name in rec["sent_to"]:
                continue
            batch.append((key, to_hec(tcfg, ev)))
        if not batch:
            print("  %s: nothing new" % name)
            continue
        if args.dry_run:
            print("  (dry-run) %s: %d events" % (name, len(batch)))
            for _, e in batch[:3]:
                print("    " + json.dumps(e["event"]))
        else:
            hec_send(tcfg, [e for _, e in batch])
            for key, _ in batch:
                dedup[key]["sent_to"].append(name)
            print("  %s: sent %d events" % (name, len(batch)))
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
    load_dotenv()  # pick up WITHINGS_CLIENT_ID/SECRET etc. from tools/.env if present

    if args.auth:
        do_auth()
        return

    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        sys.exit("another withings_to_hec.py run holds the lock; exiting.")
    try:
        run_sync(args)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


if __name__ == "__main__":
    main()
