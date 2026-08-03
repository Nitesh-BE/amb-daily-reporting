// Cloudflare Worker — always-on trigger for the Ambassador bot report.
//
// INSTANT: a RingCentral webhook subscription pushes new posts to this Worker
// the moment they're typed, so /gr is acknowledged within ~1–2 seconds.
// The per-minute poll stays as a safety net (if a webhook is ever missed) and
// runs the scheduled ("/gr in 10m") reports. Both paths de-dup by post id.
//
// Multi-team: watches every team in WATCHED_TEAMS; each report posts back to the
// team the command came from. Daily (10:00 IST) posts to DAILY_GROUP_ID.

export default {
  async scheduled(event, env, ctx) {
    if (event.cron === "* * * * *") {
      ctx.waitUntil(tick(env));
    } else {
      const daily = env.DAILY_GROUP_ID || watchedTeams(env)[0];
      ctx.waitUntil(dispatch(env, 24, "scheduled daily (Cloudflare)", daily));
    }
  },

  async fetch(request, env) {
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
      const team = url.searchParams.get("team") || env.DAILY_GROUP_ID || watchedTeams(env)[0];
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
      return new Response(JSON.stringify({ alive: true, watched: watchedTeams(env), pending, sub }, null, 2) + "\n");
    }
    return new Response("AMB bot worker is alive\n");
  },
};

function watchedTeams(env) {
  return (env.WATCHED_TEAMS || env.RC_GROUP_ID || "").split(",").map(s => s.trim()).filter(Boolean);
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

// ── per-minute tick (safety net + scheduler + subscription upkeep) ───────────
async function tick(env) {
  if (!env.RC_BOT_TOKEN) return;
  await ensureSubscription(env);
  await fireDueScheduled(env);
  for (const team of watchedTeams(env)) await pollTeam(env, team);
}

// ── instant path: a post was pushed to us ───────────────────────────────────
async function handleWebhookEvent(env, post) {
  const team = String(post.groupId || "");
  if (!team || !watchedTeams(env).includes(team)) return;
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
  if (meta && meta.id) {
    try { await fetch(`${server}/restapi/v1.0/subscription/${meta.id}`, { method: "DELETE", headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}` } }); } catch {}
  }
  const address = env.WEBHOOK_URL || "https://amb-daily-reporting.maddala-nitesh.workers.dev/";
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
    body: JSON.stringify({ ref: "main", inputs: { lookback_hours: String(Math.round(hours)), group_id: groupId || "" } }),
  });
  return resp.ok;
}
