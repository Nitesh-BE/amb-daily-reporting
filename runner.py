#!/usr/bin/env python3
"""
Generate the Ambassador error report and deliver it to RingCentral.

Runs the proven gen_report_all_errors.py as a subprocess, streams its progress
into a single live-updating RingCentral message, then uploads the finished HTML
report as an attachment with a short preview summary.

Called by:
  • launchd     (weekdays 10:00 IST)  -> run_report("scheduled")
  • listener.py (/generateReport)     -> run_report("/generateReport")
  • manually    (python3 runner.py)
"""
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from rc_client import RingCentral, load_config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CFG = load_config(BASE_DIR)

PY_EXE = os.path.join(BASE_DIR, ".venv", "bin", "python3")
if not os.path.exists(PY_EXE):        # CI / no venv: use the running interpreter
    PY_EXE = sys.executable
SCRIPT = os.path.join(BASE_DIR, "gen_report_all_errors.py")
TOTAL_QUERIES = 11
UPDATE_EVERY = 3.0  # seconds between live message edits (RC rate-limit friendly)

LINE_RE = re.compile(r"^\[(?P<label>[^\]]+)\]\s*(?P<msg>.*)$")


def coralogix_key():
    env = (os.environ.get("CORALOGIX_API_KEY") or "").strip()
    if env:
        return env
    k = (CFG.get("coralogix", {}).get("api_key") or "").strip()
    if k:
        return k
    import json
    with open(os.path.join(BASE_DIR, "key_coralogix.json")) as f:
        return json.load(f)["apiKey"]["keyValue"]


def _short(label):
    # "Q3_by_system" -> "Q3 by system"
    return label.replace("_", " ", 1).replace("_", " ")


def _scheduled_done_today(rc, ist_date):
    """True if a SCHEDULED run already fired today (IST). Lets the several
    morning cron attempts de-dup to one, without a manual/ /gr run suppressing
    the daily (those carry a different trigger label in the 'generating' post)."""
    ist = timezone(timedelta(hours=5, minutes=30))
    try:
        for p in rc.recent_posts(count=20):
            t = p.get("text") or ""
            if "Ambassador Error Report" not in t or "trigger: scheduled" not in t:
                continue
            ct = p.get("creationTime", "")
            try:
                dt = datetime.strptime(ct[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                if dt.astimezone(ist).strftime("%Y-%m-%d") == ist_date:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def run_report(trigger="scheduled", lookback_hours=24):
    rc = RingCentral(CFG["ringcentral"])

    # De-dup: several backup cron times fire each morning; only the FIRST
    # scheduled run of the day posts. Manual / /gr runs are never skipped.
    ist_today = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")
    if "scheduled" in trigger.lower() and _scheduled_done_today(rc, ist_today):
        print(f"[skip] scheduled report already ran today ({ist_today})", flush=True)
        return True

    key = coralogix_key()
    base = CFG.get("coralogix", {}).get("base_url", "https://api.cx498.coralogix.com")

    now = datetime.now(timezone.utc)
    end_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_iso = (now - timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    today_iso = now.strftime("%Y-%m-%d")
    win = "" if abs(lookback_hours - 24) < 0.01 else f" · last {lookback_hours:g}h"

    header = f"🔄 **Ambassador Error Report** — generating…{win} _(trigger: {trigger})_"
    post_id = rc.post(header + "\n\n_submitting queries…_")

    order, phase = [], {}
    done = 0
    all_lines = []
    last_edit = 0.0

    def render(footer="_running…_"):
        rows = "\n".join(f"{phase[l]}  {_short(l)}" for l in order)
        bar_done = "▰" * done + "▱" * (TOTAL_QUERIES - done)
        return f"{header}\n\n{rows}\n\n{bar_done}  {done}/{TOTAL_QUERIES}\n{footer}"

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen([PY_EXE, SCRIPT, key, base, end_iso, start_iso, today_iso],
                            cwd=BASE_DIR, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1)

    for raw in proc.stdout:
        line = raw.rstrip("\n")
        all_lines.append(line)
        m = LINE_RE.match(line)
        if m:
            lab, msg = m.group("label"), m.group("msg").lower()
            if lab not in phase:
                phase[lab] = "⏳"
                order.append(lab)
            if msg.startswith("submitting"):
                phase[lab] = "⏳"
            elif "polling" in msg:
                phase[lab] = "🔁"
            elif "fetching" in msg:
                phase[lab] = "📥"
            elif msg.endswith("rows"):
                phase[lab] = "✅"
                done += 1
            elif "failed" in msg or "timed out" in msg:
                phase[lab] = "⚠️"
                done += 1
            now = time.time()
            if now - last_edit > UPDATE_EVERY:
                try:
                    rc.update(post_id, render())
                except Exception:
                    pass
                last_edit = now

    proc.wait()
    summary = parse_summary(all_lines)

    if proc.returncode != 0 or not summary.get("report_path"):
        try:
            rc.update(post_id, render(footer="❌ **Failed** — see runner log."))
        except Exception:
            pass
        rc.post("❌ Report generation failed.\n```\n" +
                "\n".join(all_lines[-15:]) + "\n```")
        return False

    # final progress state
    try:
        rc.update(post_id, render(footer="✅ **Done** — uploading report…"))
    except Exception:
        pass

    report_path = summary["report_path"]
    if not os.path.isabs(report_path):
        report_path = os.path.join(BASE_DIR, report_path)
    # Compute in TRUE IST (UTC+5:30, no DST) — not machine-local, since GitHub
    # runners are UTC. This is what showed "04:35 IST" instead of "10:05 IST".
    ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    today = summary.get("today", ist_now.strftime("%Y-%m-%d"))
    generated = ist_now.strftime("%Y-%m-%d %H:%M IST (%a)")
    nice_name = ist_now.strftime("Ambassador_Error_Report_%Y-%m-%d_%a_%H-%M_IST.html")
    preview = build_preview(summary, today, generated)
    rc.upload(report_path, text=preview, filename=nice_name)
    return True


def parse_summary(lines):
    out = {}
    for ln in lines:
        s = ln.strip()
        if s.startswith("Report:"):
            p = s.split(":", 1)[1].strip()
            out["report_path"] = p
            m = re.search(r"(\d{4}-\d{2}-\d{2})", p)
            if m:
                out["today"] = m.group(1)
        elif s.startswith("Total errors"):
            out["total"] = _num(s)
        elif s.startswith("5xx"):
            out["c5xx"] = _num(s)
        elif s.startswith("4xx"):
            out["c4xx"] = _num(s)
        elif s.startswith("Timeouts"):
            out["timeouts"] = _num(s)
        elif s.startswith("Rate-limit"):
            out["ratelimit"] = _num(s)
        elif s.startswith("Top systems"):
            out["top_systems"] = s.split(":", 1)[1].strip()
        elif s.startswith("Period"):
            out["period"] = s.split(":", 1)[1].strip()
    return out


def _num(s):
    m = re.search(r":\s*([\d,]+)", s)
    return m.group(1) if m else "0"


def build_preview(summary, today, generated):
    top = summary.get("top_systems", "")
    top = top.strip("[]").replace("'", "")
    return (
        f"✅ **Ambassador Error Report — {today}** _(last 24h)_\n"
        f"🕒 Generated: {generated}\n\n"
        f"📊 **{summary.get('total','0')}** total errors  •  "
        f"🔴 {summary.get('c5xx','0')} 5xx  •  "
        f"🟣 {summary.get('timeouts','0')} timeouts  •  "
        f"🟡 {summary.get('ratelimit','0')} rate-limited\n"
        f"🔺 Top systems: {top}\n\n"
        f"📎 Full report attached below — download and open in a browser."
    )


if __name__ == "__main__":
    trig = sys.argv[1] if len(sys.argv) > 1 else "manual"
    try:
        lb = float(os.environ.get("LOOKBACK_HOURS") or 24)
    except ValueError:
        lb = 24.0
    ok = run_report(trig, lookback_hours=lb)
    sys.exit(0 if ok else 1)
