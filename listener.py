#!/usr/bin/env python3
"""
Always-on listener that watches the RingCentral team for the `/generateReport`
command and triggers a report run (with live progress) on demand.

Uses simple polling (every few seconds) — robust, no public URL or WebSocket
connection to maintain. Only reacts to commands posted AFTER it starts.
"""
import time
import traceback

import runner
from rc_client import RingCentral, load_config

POLL_SECONDS = 5
COMMAND = "/generatereport"          # matched case-insensitively


def main():
    cfg = load_config()
    rc = RingCentral(cfg["ringcentral"])

    # Seed "seen" with existing posts so old/history commands don't fire.
    seen = {p["id"] for p in rc.recent_posts(count=40)}
    print(f"listener started — polling every {POLL_SECONDS}s for '{COMMAND}'", flush=True)

    while True:
        try:
            for p in rc.recent_posts(count=20):
                pid = p["id"]
                if pid in seen:
                    continue
                seen.add(pid)
                text = (p.get("text") or "").strip().lower()
                if text.startswith(COMMAND):
                    print(f"trigger from post {pid}", flush=True)
                    try:
                        runner.run_report(trigger="/generateReport")
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
