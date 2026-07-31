#!/usr/bin/env python3
"""
Always-on listener that watches the RingCentral team for a report command and
triggers a run (with live progress) on demand.

Commands (case-insensitive):
  /gr                 → report for the last 24h (default)
  /generateReport     → same
  /gr 6h   /gr 6      → last 6 hours
  /gr 2d              → last 2 days

Uses simple polling — robust, no public URL. Only reacts to commands posted
AFTER it starts. NOTE: only fires while this Mac is awake + online; the
guaranteed daily 10:00 report runs on GitHub Actions, not here.
"""
import re
import time
import traceback

import runner
from rc_client import RingCentral, load_config

POLL_SECONDS = 5
COMMANDS = ("/generatereport", "/gr")      # matched case-insensitively


def parse_lookback(arg):
    """'', '6', '6h', '2d' → hours (float). Default 24."""
    if not arg:
        return 24.0
    m = re.match(r"(\d+(?:\.\d+)?)\s*([hd]?)", arg)
    if not m:
        return 24.0
    n = float(m.group(1))
    return n * 24 if m.group(2) == "d" else n


def parse_command(text):
    """Return lookback hours if text is a report command, else None."""
    low = text.strip().lower()
    for cmd in COMMANDS:
        if low == cmd or low.startswith(cmd + " "):
            return parse_lookback(low[len(cmd):].strip())
    return None


def main():
    cfg = load_config()
    rc = RingCentral(cfg["ringcentral"])

    # Seed "seen" with existing posts so old/history commands don't fire.
    seen = {p["id"] for p in rc.recent_posts(count=40)}
    print(f"listener started — polling every {POLL_SECONDS}s for {COMMANDS}", flush=True)

    while True:
        try:
            for p in rc.recent_posts(count=20):
                pid = p["id"]
                if pid in seen:
                    continue
                seen.add(pid)
                hours = parse_command(p.get("text") or "")
                if hours is None:
                    continue
                print(f"trigger from post {pid} ({hours:g}h)", flush=True)
                try:
                    runner.run_report(trigger="/gr", lookback_hours=hours)
                except Exception as e:
                    traceback.print_exc()
                    try:
                        rc.post(f"❌ Report failed: {e}")
                    except Exception:
                        pass
                # Ignore anything typed while the report was running.
                for q in rc.recent_posts(count=20):
                    seen.add(q["id"])
        except Exception:
            traceback.print_exc()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
