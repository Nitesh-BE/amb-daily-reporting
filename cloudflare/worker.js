// Cloudflare Worker — always-on trigger for the Ambassador bot report.
// Crons (wrangler.toml):
//   "* * * * *"     → every minute: fire any due scheduled reports, then poll
//                     the RingCentral TEST team for new /gr commands.
//   "30 4 * * 1-5"  → 10:00 IST weekdays: the automatic daily report.
//
// Commands (typed in the team, matched by the poll):
//   /gr                → report now, last 24h
//   /gr 2h  /gr 2d     → report now, custom lookback window
//   /gr in 10m         → SCHEDULE a report ~10 min from now (last 24h)
//   /gr in 2h 6h       → schedule ~2h from now, window = last 6h
// "in <delay>" = schedule; a bare number = lookback window (never confused).
//
// Everything is scoped to the isolated bot pipeline; never touches the live report.

export default {
  async scheduled(event, env, ctx) {
    if (event.cron === "* * * * *") {
      ctx.waitUntil(tick(env));                                    // listener + scheduler
    } else {
      ctx.waitUntil(dispatch(env, 24, "scheduled daily (Cloudflare)"));  // the daily
    }
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.searchParams.get("run")) {
      const h = parseFloat(url.searchParams.get("hours") || "24") || 24;
      const ok = await dispatch(env, h, "manual (Worker URL)");
      return new Response(ok ? `dispatched (lookback ${h}h)\n` : "GITHUB_TOKEN not set\n");
    }
    if (url.searchParams.get("status")) {
      const pending = JSON.parse((await env.LASTSEEN.get("scheduled")) || "[]");
      return new Response(JSON.stringify({ alive: true, pending }, null, 2) + "\n");
    }
    return new Response("AMB bot worker is alive\n");
  },
};

// ── command parsing ──────────────────────────────────────────────────────────
function parseCommand(text) {
  const low = (text || "").trim().toLowerCase();
  const m = low.match(/^\/(?:gr|generatereport)\b(.*)$/);
  if (!m) return null;
  let rest = m[1].trim();
  let delayMin = 0;
  const inM = rest.match(/^in\s+(\d+(?:\.\d+)?)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)\b(.*)$/);
  if (inM) {
    const n = parseFloat(inM[1]);
    const u = inM[2][0];                         // m / h / d
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

// ── the per-minute tick: fire due schedules, then read new commands ──────────
async function tick(env) {
  if (!env.RC_BOT_TOKEN || !env.RC_GROUP_ID) return;
  await fireDueScheduled(env);

  const server = (env.RC_SERVER_URL || "https://platform.ringcentral.com").replace(/\/+$/, "");
  const r = await fetch(`${server}/team-messaging/v1/chats/${env.RC_GROUP_ID}/posts?recordCount=10`,
    { headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}` } });
  if (!r.ok) return;
  const posts = (await r.json()).records || [];

  const lastTs = parseInt((await env.LASTSEEN.get("lastTs")) || "0", 10);
  if (!lastTs) {                                  // first run: seed, don't replay history
    let seed = 0;
    for (const p of posts) seed = Math.max(seed, Date.parse(p.creationTime || "") || 0);
    await env.LASTSEEN.put("lastTs", String(seed || Date.now()));
    return;
  }

  let maxTs = lastTs;
  const actions = [];
  for (const p of posts) {
    const ct = Date.parse(p.creationTime || "") || 0;
    if (ct <= lastTs) continue;
    maxTs = Math.max(maxTs, ct);
    const cmd = parseCommand(p.text);
    if (cmd) actions.push(cmd);
  }
  if (maxTs > lastTs) await env.LASTSEEN.put("lastTs", String(maxTs));

  for (const cmd of actions) {
    if (cmd.delayMin > 0) {
      await scheduleReport(env, cmd);
      await postToTeam(env, `⏳ Scheduled — report in ~${fmtDelay(cmd.delayMin)} (window: last ${cmd.lookbackH}h).`);
    } else {
      await dispatch(env, cmd.lookbackH, "/gr (Cloudflare)");
    }
  }
}

// ── scheduled-report store (Cloudflare KV) ───────────────────────────────────
async function scheduleReport(env, cmd) {
  let list = [];
  try { list = JSON.parse((await env.LASTSEEN.get("scheduled")) || "[]") || []; } catch {}
  list.push({ fireAt: Date.now() + cmd.delayMin * 60000, lookbackH: cmd.lookbackH });
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
      await postToTeam(env, `▶️ Running your scheduled report now (window: last ${s.lookbackH}h).`);
      await dispatch(env, s.lookbackH, "scheduled /gr (Cloudflare)");
    } else {
      remaining.push(s);
    }
  }
  if (remaining.length !== list.length) await env.LASTSEEN.put("scheduled", JSON.stringify(remaining));
}

// ── helpers ──────────────────────────────────────────────────────────────────
async function postToTeam(env, text) {
  const server = (env.RC_SERVER_URL || "https://platform.ringcentral.com").replace(/\/+$/, "");
  try {
    await fetch(`${server}/team-messaging/v1/chats/${env.RC_GROUP_ID}/posts`, {
      method: "POST",
      headers: { Authorization: `Bearer ${env.RC_BOT_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
  } catch {}
}

async function dispatch(env, hours, reason) {
  const token = env.GITHUB_TOKEN;
  if (!token) return false;
  const repo = env.GITHUB_REPO || "Nitesh-BE/amb-daily-reporting";
  const wf = env.GITHUB_WORKFLOW || "daily-report-bot.yml";
  const resp = await fetch(
    `https://api.github.com/repos/${repo}/actions/workflows/${wf}/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "amb-bot-worker",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref: "main", inputs: { lookback_hours: String(Math.round(hours)) } }),
    });
  return resp.ok;
}
