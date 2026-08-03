// Cloudflare Worker — always-on trigger for the Ambassador bot report.
// Two cron jobs (see wrangler.toml):
//   "* * * * *"      → every minute: poll the RingCentral TEST team for /gr and,
//                      on a new command, trigger the GitHub bot workflow.
//   "45 3 * * 1-5"   → 09:15 IST weekdays: reliably trigger the daily report.
// Also exposes a URL for manual testing: GET /?run=1[&hours=6].
//
// Everything is scoped to the isolated bot pipeline (bot token + test team +
// daily-report-bot.yml). It never touches the live report.

const CMD_RE = /^\/(?:gr|generatereport)\b\s*(\d+(?:\.\d+)?)?\s*([hd]?)/;

export default {
  async scheduled(event, env, ctx) {
    if (event.cron === "* * * * *") {
      ctx.waitUntil(pollForCommands(env));                        // every minute: /gr listener
    } else {
      ctx.waitUntil(dispatch(env, 24, "scheduled daily (Cloudflare)"));  // the daily cron (any other schedule)
    }
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.searchParams.get("run")) {
      const h = parseFloat(url.searchParams.get("hours") || "24") || 24;
      const ok = await dispatch(env, h, "manual (Worker URL)");
      return new Response(ok ? `dispatched (lookback ${h}h)\n` : "GITHUB_TOKEN not set\n");
    }
    return new Response("AMB bot worker is alive\n");
  },
};

async function pollForCommands(env) {
  const server = (env.RC_SERVER_URL || "https://platform.ringcentral.com").replace(/\/+$/, "");
  const group = env.RC_GROUP_ID;
  const token = env.RC_BOT_TOKEN;
  if (!token || !group) return;

  const r = await fetch(`${server}/team-messaging/v1/chats/${group}/posts?recordCount=10`,
    { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) return;
  const posts = (await r.json()).records || [];

  const lastTs = parseInt((await env.LASTSEEN.get("lastTs")) || "0", 10);
  if (!lastTs) {                       // first run: seed to newest, don't replay history
    let seed = 0;
    for (const p of posts) seed = Math.max(seed, Date.parse(p.creationTime || "") || 0);
    await env.LASTSEEN.put("lastTs", String(seed || Date.now()));
    return;
  }

  let maxTs = lastTs, chosen = null;
  for (const p of posts) {
    const ct = Date.parse(p.creationTime || "") || 0;
    if (ct <= lastTs) continue;
    maxTs = Math.max(maxTs, ct);
    const m = (p.text || "").trim().toLowerCase().match(CMD_RE);
    if (m) chosen = m;
  }
  if (maxTs > lastTs) await env.LASTSEEN.put("lastTs", String(maxTs));

  if (chosen) {
    const hours = chosen[1] ? parseFloat(chosen[1]) * (chosen[2] === "d" ? 24 : 1) : 24;
    await dispatch(env, hours, "/gr (Cloudflare)");
  }
}

async function dispatch(env, hours, reason) {
  const token = env.GITHUB_TOKEN;
  if (!token) return false;            // not wired yet
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
