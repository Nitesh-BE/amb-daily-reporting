#!/usr/bin/env python3
"""Minimal RingCentral Team Messaging client (JWT auth flow).

Handles token exchange/caching and the few operations the daily report needs:
posting a message, live-updating a message, uploading a file as an attachment,
and reading recent posts (for the /generateReport poll trigger).
"""
import json
import os
import threading
import time
from urllib.parse import quote

import requests


class RingCentral:
    def __init__(self, cfg):
        self.server = cfg["server_url"].rstrip("/")
        self.client_id = cfg["client_id"]
        self.client_secret = cfg["client_secret"]
        self.jwt = cfg["jwt"]
        # If a (near-permanent) bot token is present, we post AS the bot.
        self.bot_token = (cfg.get("bot_token") or "").strip()
        self.group_id = str(cfg.get("group_id") or "")
        self._token = None
        self._token_exp = 0.0
        self._lock = threading.Lock()

    # ── auth ───────────────────────────────────────────────────────────────
    def token(self):
        if self.bot_token:                 # bot identity: static, near-permanent
            return self.bot_token
        with self._lock:
            if self._token and time.time() < self._token_exp - 60:
                return self._token
            r = requests.post(
                f"{self.server}/restapi/oauth/token",
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                      "assertion": self.jwt},
                timeout=30)
            r.raise_for_status()
            j = r.json()
            self._token = j["access_token"]
            self._token_exp = time.time() + int(j.get("expires_in", 3600))
            return self._token

    def _h(self, json_body=False):
        h = {"Authorization": f"Bearer {self.token()}"}
        if json_body:
            h["Content-Type"] = "application/json"
        return h

    def _chat(self, chat_id=None):
        return str(chat_id or self.group_id)

    # ── messaging ──────────────────────────────────────────────────────────
    def post(self, text, chat_id=None):
        """Create a post; returns its id."""
        r = requests.post(
            f"{self.server}/team-messaging/v1/chats/{self._chat(chat_id)}/posts",
            headers=self._h(True), data=json.dumps({"text": text}), timeout=30)
        r.raise_for_status()
        return r.json()["id"]

    def update(self, post_id, text, chat_id=None):
        """Live-edit an existing post (used for streaming progress)."""
        r = requests.patch(
            f"{self.server}/team-messaging/v1/chats/{self._chat(chat_id)}/posts/{post_id}",
            headers=self._h(True), data=json.dumps({"text": text}), timeout=30)
        r.raise_for_status()
        return r.json()

    def upload(self, file_path, text="", chat_id=None, filename=None):
        """Upload a file (multipart) and post it as an attachment; returns post id."""
        chat = self._chat(chat_id)
        name = filename or os.path.basename(file_path)
        with open(file_path, "rb") as f:
            data = f.read()
        # NB: no groupId -> the file is only stored (not auto-posted), so the
        # single post below is the only message that appears (text + attachment).
        up = requests.post(
            f"{self.server}/restapi/v1.0/glip/files?name={quote(name)}",
            headers=self._h(), files={"file": (name, data, "text/html")}, timeout=120)
        up.raise_for_status()
        j = up.json()
        rec = j[0] if isinstance(j, list) else j["records"][0]
        file_id = rec["id"]
        r = requests.post(
            f"{self.server}/team-messaging/v1/chats/{chat}/posts",
            headers=self._h(True),
            data=json.dumps({"text": text,
                             "attachments": [{"id": file_id, "type": "File"}]}),
            timeout=30)
        r.raise_for_status()
        return r.json()["id"]

    def recent_posts(self, chat_id=None, count=20):
        r = requests.get(
            f"{self.server}/team-messaging/v1/chats/{self._chat(chat_id)}/posts?recordCount={count}",
            headers=self._h(), timeout=30)
        r.raise_for_status()
        return r.json().get("records", [])


def load_config(base_dir=None):
    # In CI (GitHub Actions) secrets arrive as env vars — no config.json on disk.
    if os.environ.get("RC_CLIENT_ID") or os.environ.get("SLACK_BOT_TOKEN"):
        return {
            "messenger": os.environ.get("MESSENGER", "ringcentral"),
            "slack": {
                "bot_token": os.environ.get("SLACK_BOT_TOKEN", ""),
                "channel_id": os.environ.get("SLACK_CHANNEL_ID", ""),
            },
            "ringcentral": {
                "server_url": os.environ.get("RC_SERVER_URL", "https://platform.ringcentral.com"),
                "client_id": os.environ.get("RC_CLIENT_ID", ""),
                "client_secret": os.environ.get("RC_CLIENT_SECRET", ""),
                "jwt": os.environ.get("RC_JWT", ""),
                "bot_token": os.environ.get("RC_BOT_TOKEN", ""),
                "group_id": os.environ.get("RC_GROUP_ID", ""),
            },
            "coralogix": {
                "api_key": os.environ.get("CORALOGIX_API_KEY", ""),
                "base_url": os.environ.get("CORALOGIX_BASE_URL", "https://api.cx498.coralogix.com"),
            },
        }
    base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base_dir, "config.json")) as f:
        return json.load(f)


def get_messenger(cfg):
    """Slack or RingCentral client, chosen by MESSENGER env / cfg["messenger"]."""
    which = (os.environ.get("MESSENGER") or cfg.get("messenger") or "ringcentral").lower()
    if which == "slack":
        from slack_client import Slack
        return Slack(cfg["slack"])
    return RingCentral(cfg["ringcentral"])
