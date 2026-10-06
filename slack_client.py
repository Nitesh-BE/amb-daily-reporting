#!/usr/bin/env python3
"""Minimal Slack client with the same interface as rc_client.RingCentral.

post / update / upload / recent_posts, so runner.py works unchanged against
either platform. Messages are written in RingCentral-style markdown
(**bold**); this client converts them to Slack mrkdwn (*bold*).
"""
import os
import re
from datetime import datetime, timezone

import requests

API = "https://slack.com/api"


def to_mrkdwn(text):
    """RC markdown -> Slack mrkdwn: escape &,<,> and turn **bold** into *bold*."""
    text = (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)


class Slack:
    def __init__(self, cfg):
        self.bot_token = cfg["bot_token"].strip()
        self.channel = str(cfg.get("channel_id") or "")

    def _h(self):
        return {"Authorization": f"Bearer {self.bot_token}"}

    def _call(self, method, http="post", **kw):
        fn = requests.post if http == "post" else requests.get
        r = fn(f"{API}/{method}", headers=self._h(), timeout=60, **kw)
        r.raise_for_status()
        j = r.json()
        if not j.get("ok"):
            raise RuntimeError(f"Slack {method} failed: {j.get('error')}")
        return j

    def _chat(self, chat_id=None):
        return str(chat_id or self.channel)

    # ── messaging ──────────────────────────────────────────────────────────
    def post(self, text, chat_id=None):
        """Create a message; returns its ts (Slack's message id)."""
        j = self._call("chat.postMessage",
                       json={"channel": self._chat(chat_id), "text": to_mrkdwn(text)})
        return j["ts"]

    def update(self, post_id, text, chat_id=None):
        """Live-edit an existing message (used for streaming progress)."""
        return self._call("chat.update",
                          json={"channel": self._chat(chat_id), "ts": post_id,
                                "text": to_mrkdwn(text)})

    def upload(self, file_path, text="", chat_id=None, filename=None):
        """Upload a file and share it to the channel with `text` as the comment."""
        name = filename or os.path.basename(file_path)
        with open(file_path, "rb") as f:
            data = f.read()
        # 1) reserve an upload URL  2) send the bytes  3) complete + share
        j = self._call("files.getUploadURLExternal", http="get",
                       params={"filename": name, "length": len(data)})
        up = requests.post(j["upload_url"], files={"file": (name, data, "text/html")},
                           timeout=120)
        up.raise_for_status()
        self._call("files.completeUploadExternal",
                   json={"files": [{"id": j["file_id"], "title": name}],
                         "channel_id": self._chat(chat_id),
                         "initial_comment": to_mrkdwn(text)})
        return j["file_id"]

    def recent_posts(self, chat_id=None, count=20):
        """Recent messages as RC-shaped records: {id, text, creationTime}."""
        j = self._call("conversations.history", http="get",
                       params={"channel": self._chat(chat_id), "limit": count})
        out = []
        for m in j.get("messages", []):
            ct = datetime.fromtimestamp(float(m["ts"]), timezone.utc)
            out.append({"id": m["ts"], "text": m.get("text", ""),
                        "creationTime": ct.strftime("%Y-%m-%dT%H:%M:%S.000Z")})
        return out
