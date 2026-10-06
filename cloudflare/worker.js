// Cloudflare Worker — always-on trigger for the Ambassador bot report.
//
// INSTANT: a RingCentral webhook subscription pushes new posts to this Worker
// the moment they're typed, so /gr is acknowledged within ~1–2 seconds.
// The per-minute poll stays as a safety net (if a webhook is ever missed) and
// runs the scheduled ("/gr in 10m") reports. Both paths de-dup by post id.
//
// Multi-team: watches every team in WATCHED_TEAMS; each report posts back to the
// team the command came from. Daily (10:00 IST) posts to DAILY_GROUP_ID.
//
// Slack: /gr is a Slack slash command POSTed to /slack/command (signed with
// SLACK_SIGNING_SECRET). Slack channel ids (C…/G…) route to Slack, numeric
// ids to RingCentral, so both platforms run side by side during the cutover.

export default {
  async scheduled(event, env, ctx) {
    // Single cron now (`* * * * *`): the every-minute tick drives BOTH the /gr
    // poll AND the daily send (see maybeRunDaily). Any cron invocation → tick,
    // so even a stray/legacy daily cron just runs a guarded tick, never a double.
    // This is deliberate: the per-minute cron is the one trigger that has proven
    // reliable, so the 10:00 daily now rides on it instead of a separate cron
    // (a separate `30 4 * * 1-5` cron was silently dropped by a redeploy once).
    ctx.waitUntil(tick(env));
  },

  async fetch(request, env, ctx) {
    // 0) Slack slash command (/gr) — Slack POSTs a signed form here
    if (request.method === "POST" && new URL(request.url).pathname === "/slack/command") {
      return handleSlackCommand(request, env, ctx);
    }
    // 1) RingCentral webhook validation handshake
    const vt = request.headers.get("Validation-Token");
    if (vt) return new Response("", { status: 200, headers: { "Validation-Token": vt } });
    // 2) RingCentral event push (instant path)
    if (request.method === "POST") {
      let body = null; try { body = await request.json(); } catch {}
      if (body && body.body) await handleWebhookEvent(env, body.body);
      return new Response("ok\n");
    }
    // 3) GET: health / status / manual run
    const url = new URL(request.url);
    if (url.searchParams.get("run")) {
      const h = parseFloat(url.searchParams.get("hours") || "24") || 24;
      const team = url.searchParams.get("team") || (await dailyTargets(env))[0];
      const ok = await dispatch(env, h, "manual (URL)", team);
      return new Response(ok ? `dispatched (team ${team}, ${h}h)\n` : "GITHUB_TOKEN not set\n");
    }
    if (url.searchParams.get("sub")) {
      if (url.searchParams.get("sub") === "force") await env.LASTSEEN.delete("sub");
      await ensureSubscription(env);
      return new Response(JSON.stringify(JSON.parse((await env.LASTSEEN.get("sub")) || "null"), null, 2) + "\n");
    }
    if (url.searchParams.get("status")) {
      const pending = JSON.parse((await env.LASTSEEN.get("scheduled")) || "[]");
      const sub = JSON.parse((await env.LASTSEEN.get("sub")) || "null");
      return new Response(JSON.stringify({ alive: true, dailyTargets: await dailyTargets(env), pending, sub }, null, 2) + "\n");
    }
    return new Response("AMB bot worker is alive\n");
  },
};

// Teams the bot serves = the teams it's actually a MEMBER of. We learn each one
// the first time the webhook delivers a post from it (the bot only receives
// events for teams it belongs to). WATCHED_TEAMS is an optional always-on seed.
async function getMemberTeams(env) {
  const set = new Set((env.WATCHED_TEAMS || env.RC_GROUP_ID || "").split(",").map(s => s.trim()).filter(Boolean));
  try {
    const m = JSON.parse((await env.LASTSEEN.get("memberTeams")) || "{}") || {};
    const cutoff = Date.now() - 60 * 24 * 3600 * 1000;   // forget teams silent > 60 days
    for (const [t, ts] of Object.entries(m)) if (ts > cutoff) set.add(t);
  } catch {}
  const excl = new Set((env.EXCLUDE_TEAMS || "1851899910").split(",").map(s => s.trim()).filter(Boolean));
  return [...set].filter(t => t && !excl.has(t));
}
async function learnTeam(env, team) {
  const excl = new Set((env.EXCLUDE_TEAMS || "1851899910").split(",").map(s => s.trim()));
  if (!team || excl.has(team)) return;
  let m = {};
  try { m = JSON.parse((await env.LASTSEEN.get("memberTeams")) || "{}") || {}; } catch {}
  m[team] = Date.now();
  await env.LASTSEEN.put("memberTeams", JSON.stringify(m));
}
function rcServer(env) {
  return (env.RC_SERVER_URL || "https://platform.ringcentral.com").replace(/\/+$/, "");
}

function parseCommand(text) {
  const low = (text || "").trim().toLowerCase();
  const m = low.match(/^\/(?:gr|generatereport)\b(.*)$/);
  if (!m) return null;
  let rest = m[1].trim();
  let delayMin = 0;
  const inM = rest.match(/^in\s+(\d+(?:\.\d+)?)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)\b(.*)$/);
  if (inM) {
    const n = parseFloat(inM[1]), u = inM[2][0];
    delayMin = u === "m" ? n : u === "h" ? n * 60 : n * 1440;
    rest = inM[3].trim();
  }
  let lookbackH = 24;
  const lbM = rest.match(/^(\d+(?:\.\d+)?)\s*([hd])?/);
  if (lbM && lbM[1]) lookbackH = lbM[2] === "d" ? parseFloat(lbM[1]) * 24 : parseFloat(lbM[1]);
  return { delayMin, lookbackH };
}
function fmtDelay(min) {
  if (min < 60) return `${Math.round(min)} min`;
  const h = Math.floor(min / 60), m = Math.round(min % 60);
  return m ? `${h}h ${m}m` : `${h}h`;
}

// ── per-minute tick (poll + scheduler + subscription upkeep + daily) ─────────
async function tick(env) {
  if (rcEnabled(env)) {
    await ensureSubscription(env);
    for (const team of await getMemberTeams(env)) await pollTeam(env, team);
  }
  await fireDueScheduled(env);
  await maybeRunDaily(env);
}

function rcEnabled(env) {
  return !!env.RC_BOT_TOKEN && env.RC_ENABLED !== "off";
}

// Teams/channels that get the 10:00 IST daily: RingCentral teams unless
// RC_ENABLED="off", plus every Slack channel the bot is in when SLACK_DAILY="on".
async function dailyTargets(env) {
  const out = [];
  if (rcEnabled(env)) out.push(...(await getMemberTeams(env)));
  if (env.SLACK_BOT_TOKEN && env.SLACK_DAILY === "on") out.push(...(await slackMemberChannels(env)));
  return out;
}

// ── the 10:00 IST daily, run FROM the reliable every-minute tick ─────────────
// Three phases, each guarded by a KV flag so it runs exactly once per IST day:
//   A (>=10:00) send today's report to every member team.
//   B (>=10:20) read each team back; re-fire any that don't actually have it
//               (covers a cancelled/failed GitHub run — no silent per-team miss).
//   C (>=10:40) if a team is STILL missing, post a visible alert — a miss can
//               never again be silent (you stop being the monitor).
// Because it's driven by the per-minute cron, a skipped minute only delays a
// phase by <=1 min instead of losing the whole day.
async function maybeRunDaily(env) {
  const ist = new Date(Date.now() + 5.5 * 3600 * 1000);   // shift so UTC getters read IST
  const dow = ist.getUTCDay();                            // 0=Sun .. 6=Sat (IST)
  if (dow === 0 || dow === 6) return;                     // weekdays only
  const mins = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  if (mins < 600) return;                                 // before 10:00 IST
  const date = ist.toISOString().slice(0, 10);            // YYYY-MM-DD (IST)
  const TTL = { expirationTtl: 2 * 24 * 3600 };

  // Phase A — send once, at/after 10:00
  if (!(await env.LASTSEEN.get(`dsent:${date}`))) {
    for (const team of await dailyTargets(env)) {
      await dispatch(env, 24, "scheduled daily (Cloudflare)", team);
    }
    await env.LASTSEEN.put(`dsent:${date}`, String(Date.now()), TTL);
    return;                                               // give reports time to generate
  }

  // Phase B — verify + retry once, at/after 10:20
  if (mins >= 620 && !(await env.LASTSEEN.get(`dverify:${date}`))) {
    for (const team of await dailyTargets(env)) {
      if (!(await teamHasReportToday(env, team, date))) {
        await dispatch(env, 24, "scheduled daily retry (Cloudflare)", team);
      }
    }
    await env.LASTSEEN.put(`dverify:${date}`, String(Date.now()), TTL);
    return;
  }

  // Phase C — alert on anything still missing, once, at/after 10:40
  if (mins >= 640 && !(await env.LASTSEEN.get(`dalert:${date}`))) {
    for (const team of await dailyTargets(env)) {
      if (!(await teamHasReportToday(env, team, date))) {
        await postToTeam(env, team,
          "⚠️ Today's automated 10:00 IST report couldn't be generated after a retry. " +
          "Flagging so it isn't a silent miss — send `/gr` to retry manually.");
      }
    }
    await env.LASTSEEN.put(`dalert:${date}`, String(Date.now()), TTL);
  }
}

// True if this team already has today's finished report (matches the posted header).
async function teamHasReportToday(env, team, date) {
  if (isSlack(team)) {
    try {
      const j = await slackApi(env, "conversations.history", { channel: team, limit: 15 }, "GET");
      return (j.messages || []).some(m => { const t = m.text || ""; return t.includes("Ambassador Error Report") && t.includes(date); });
    } catch { return false; }
  }
  try {
    const r = await fetch(`${rcServer(env)}/team-messaging/v1/chats/${team}/posts?recordCount=15`,
      { headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}` } });
    if (!r.ok) return false;
    const posts = (await r.json()).records || [];
    return posts.some(p => { const t = p.text || ""; return t.includes("Ambassador Error Report") && t.includes(date); });
  } catch { return false; }
}

// ── instant path: a post was pushed to us ───────────────────────────────────
async function handleWebhookEvent(env, post) {
  const team = String(post.groupId || "");
  const excl = new Set((env.EXCLUDE_TEAMS || "1851899910").split(",").map(s => s.trim()));
  if (!team || excl.has(team)) return;         // ignore the org-wide "Everyone" team
  await learnTeam(env, team);                  // any team we get events from = a member the bot serves
  await processCommandPost(env, { id: post.id, text: post.text, creationTime: post.creationTime }, team);
}

// ── de-duplicated command handling (shared by webhook + poll) ────────────────
async function processCommandPost(env, post, team) {
  const cmd = parseCommand(post.text);
  if (!cmd) return;
  const dkey = "done:" + post.id;
  if (await env.LASTSEEN.get(dkey)) return;                 // already handled by the other path
  await env.LASTSEEN.put(dkey, "1", { expirationTtl: 900 });
  if (cmd.delayMin > 0) {
    await scheduleReport(env, cmd, Date.parse(post.creationTime || "") || Date.now(), team);
    await postToTeam(env, team, `⏳ Scheduled — report in ~${fmtDelay(cmd.delayMin)} (window: last ${cmd.lookbackH}h).`);
  } else {
    const win = cmd.lookbackH === 24 ? "" : ` (last ${cmd.lookbackH}h)`;
    await postToTeam(env, team, `👀 Got it — generating your report${win} now, it'll be here in a few minutes…`);
    await dispatch(env, cmd.lookbackH, "/gr", team);
  }
}

async function pollTeam(env, team) {
  const r = await fetch(`${rcServer(env)}/team-messaging/v1/chats/${team}/posts?recordCount=10`,
    { headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}` } });
  if (!r.ok) return;
  const posts = (await r.json()).records || [];
  const key = `lastTs:${team}`;
  const lastTs = parseInt((await env.LASTSEEN.get(key)) || "0", 10);
  if (!lastTs) {
    let seed = 0;
    for (const p of posts) seed = Math.max(seed, Date.parse(p.creationTime || "") || 0);
    await env.LASTSEEN.put(key, String(seed || Date.now()));
    return;
  }
  let maxTs = lastTs;
  for (const p of posts) {
    const ct = Date.parse(p.creationTime || "") || 0;
    if (ct <= lastTs) continue;
    maxTs = Math.max(maxTs, ct);
    if (parseCommand(p.text)) await processCommandPost(env, p, team);
  }
  if (maxTs > lastTs) await env.LASTSEEN.put(key, String(maxTs));
}

// ── scheduled store (KV) ─────────────────────────────────────────────────────
async function scheduleReport(env, cmd, baseTs, team) {
  let list = [];
  try { list = JSON.parse((await env.LASTSEEN.get("scheduled")) || "[]") || []; } catch {}
  list.push({ fireAt: (baseTs || Date.now()) + cmd.delayMin * 60000, lookbackH: cmd.lookbackH, team });
  await env.LASTSEEN.put("scheduled", JSON.stringify(list));
}
async function fireDueScheduled(env) {
  let list;
  try { list = JSON.parse((await env.LASTSEEN.get("scheduled")) || "[]"); } catch { return; }
  if (!Array.isArray(list) || !list.length) return;
  const now = Date.now();
  const remaining = [];
  for (const s of list) {
    if (s.fireAt <= now) {
      await postToTeam(env, s.team, `▶️ Running your scheduled report now (window: last ${s.lookbackH}h).`);
      await dispatch(env, s.lookbackH, "scheduled /gr (Cloudflare)", s.team);
    } else remaining.push(s);
  }
  if (remaining.length !== list.length) await env.LASTSEEN.put("scheduled", JSON.stringify(remaining));
}

// ── RingCentral webhook subscription (create + auto-renew) ───────────────────
async function ensureSubscription(env) {
  const now = Date.now();
  let meta = null;
  try { meta = JSON.parse((await env.LASTSEEN.get("sub")) || "null"); } catch {}
  if (meta && meta.nextCheck && meta.nextCheck > now) return;   // still good / backing off

  const server = rcServer(env);
  const address = env.WEBHOOK_URL || "https://amb-daily-reporting.maddala-nitesh.workers.dev/";
  // Delete ANY existing subscriptions pointing at us, so we never accumulate duplicates.
  try {
    const list = await (await fetch(`${server}/restapi/v1.0/subscription`,
      { headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}` } })).json();
    for (const s of (list.records || [])) {
      if ((s.deliveryMode || {}).address === address) {
        await fetch(`${server}/restapi/v1.0/subscription/${s.id}`,
          { method: "DELETE", headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}` } });
      }
    }
  } catch {}
  const resp = await fetch(`${server}/restapi/v1.0/subscription`, {
    method: "POST",
    headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      eventFilters: ["/restapi/v1.0/glip/posts"],
      deliveryMode: { transportType: "WebHook", address },
      expiresIn: 604800,
    }),
  });
  if (resp.ok) {
    const j = await resp.json();
    const exp = Date.parse(j.expirationTime || "") || (now + 6 * 24 * 3600 * 1000);
    await env.LASTSEEN.put("sub", JSON.stringify({ id: j.id, expiresAt: exp, nextCheck: exp - 24 * 3600 * 1000 }));
  } else {
    await env.LASTSEEN.put("sub", JSON.stringify({ id: null, error: (await resp.text()).slice(0, 160), nextCheck: now + 15 * 60 * 1000 }));
  }
}

// ── helpers ──────────────────────────────────────────────────────────────────
async function postToTeam(env, team, text) {
  if (isSlack(team)) {
    try { await slackApi(env, "chat.postMessage", { channel: team, text }); } catch {}
    return;
  }
  try {
    await fetch(`${rcServer(env)}/team-messaging/v1/chats/${team}/posts`, {
      method: "POST",
      headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
  } catch {}
}
async function dispatch(env, hours, reason, groupId) {
  const token = env.GITHUB_TOKEN;
  if (!token) return false;
  const repo = env.GITHUB_REPO || "Nitesh-BE/amb-daily-reporting";
  const wf = env.GITHUB_WORKFLOW || "daily-report-bot.yml";
  const resp = await fetch(`https://api.github.com/repos/${repo}/actions/workflows/${wf}/dispatches`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "amb-bot-worker",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref: "main", inputs: {
      lookback_hours: String(Math.round(hours)),
      group_id: groupId || "",
      messenger: isSlack(groupId) ? "slack" : "ringcentral",
    } }),
  });
  return resp.ok;
}

// ── Slack ────────────────────────────────────────────────────────────────────
// Slack channel ids look like C0123ABCD / G0123ABCD; RingCentral team ids are numeric.
function isSlack(id) {
  return /^[CG][A-Z0-9]{6,}$/.test(String(id || ""));
}

async function slackApi(env, method, params, http = "POST") {
  const init = { method: http, headers: { Authorization: `Bearer ${env.SLACK_BOT_TOKEN}` } };
  let url = `https://slack.com/api/${method}`;
  if (http === "GET") url += "?" + new URLSearchParams(params);
  else { init.headers["Content-Type"] = "application/json; charset=utf-8"; init.body = JSON.stringify(params); }
  const j = await (await fetch(url, init)).json();
  if (!j.ok) throw new Error(`slack ${method}: ${j.error}`);
  return j;
}

// Every channel the bot has been /invite'd to.
async function slackMemberChannels(env) {
  try {
    const j = await slackApi(env, "users.conversations",
      { types: "public_channel,private_channel", exclude_archived: "true", limit: "200" }, "GET");
    return (j.channels || []).map(c => c.id);
  } catch { return []; }
}

// Constant-time-ish check of X-Slack-Signature (v0=HMAC_SHA256(secret, "v0:ts:body")).
async function verifySlack(env, request, body) {
  const ts = request.headers.get("X-Slack-Request-Timestamp") || "";
  const sig = request.headers.get("X-Slack-Signature") || "";
  if (!env.SLACK_SIGNING_SECRET || !ts || Math.abs(Date.now() / 1000 - Number(ts)) > 300) return false;
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(env.SLACK_SIGNING_SECRET),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(`v0:${ts}:${body}`));
  const expected = "v0=" + [...new Uint8Array(mac)].map(b => b.toString(16).padStart(2, "0")).join("");
  if (expected.length !== sig.length) return false;
  let diff = 0;
  for (let i = 0; i < sig.length; i++) diff |= expected.charCodeAt(i) ^ sig.charCodeAt(i);
  return diff === 0;
}

// /gr [6h | 2d | in 10m] — must answer within 3s, so the GitHub dispatch runs after we reply.
async function handleSlackCommand(request, env, ctx) {
  const body = await request.text();
  if (!(await verifySlack(env, request, body))) return new Response("bad signature", { status: 401 });
  const form = new URLSearchParams(body);
  const channel = form.get("channel_id") || "";
  const cmd = parseCommand(`/gr ${form.get("text") || ""}`);
  const reply = (text, inChannel = true) => new Response(
    JSON.stringify({ response_type: inChannel ? "in_channel" : "ephemeral", text }),
    { headers: { "Content-Type": "application/json" } });

  if (!channel.startsWith("C") && !channel.startsWith("G")) {
    return reply("Please run `/gr` in a channel (not a DM), after `/invite @AMB Daily Reporting`.", false);
  }
  if (!(await slackMemberChannels(env)).includes(channel)) {
    return reply("I'm not in this channel yet — run `/invite @AMB Daily Reporting` first, then `/gr` again.", false);
  }
  if (cmd.delayMin > 0) {
    await scheduleReport(env, cmd, Date.now(), channel);
    return reply(`⏳ Scheduled — report in ~${fmtDelay(cmd.delayMin)} (window: last ${cmd.lookbackH}h).`);
  }
  ctx.waitUntil(dispatch(env, cmd.lookbackH, "/gr (Slack)", channel));
  const win = cmd.lookbackH === 24 ? "" : ` (last ${cmd.lookbackH}h)`;
  return reply(`👀 Got it — generating your report${win} now, it'll be here in a few minutes…`);
}
