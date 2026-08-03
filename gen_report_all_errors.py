#!/usr/bin/env python3
import sys
import json
import time
import os
import requests
from datetime import datetime, timedelta, timezone

API_KEY = sys.argv[1]
BASE    = sys.argv[2] if len(sys.argv) > 2 else "https://api.cx498.coralogix.com"

_now   = datetime.now(timezone.utc)
_start = _now - timedelta(hours=24)
END       = sys.argv[3] if len(sys.argv) > 3 else _now.strftime("%Y-%m-%dT%H:%M:%SZ")
START     = sys.argv[4] if len(sys.argv) > 4 else _start.strftime("%Y-%m-%dT%H:%M:%SZ")
TODAY     = sys.argv[5] if len(sys.argv) > 5 else _now.strftime("%Y-%m-%d")
YESTERDAY = (datetime.strptime(TODAY, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

# Window label — dynamic, so custom /gr Nh|Nd ranges are labelled correctly.
try:
    _wh = (datetime.strptime(END, "%Y-%m-%dT%H:%M:%SZ")
           - datetime.strptime(START, "%Y-%m-%dT%H:%M:%SZ")).total_seconds() / 3600
    WINDOW_LABEL = (f"Last {int(round(_wh))} Hours" if _wh < 47.9
                    else f"Last {int(round(_wh / 24))} Days")
except Exception:
    WINDOW_LABEL = "Last 24 Hours"

# Display everything in IST (UTC+5:30, no DST) — START/END arrive as UTC ISO.
_IST = timezone(timedelta(hours=5, minutes=30))
def _to_ist(iso_z):
    try:
        return (datetime.strptime(iso_z, "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=timezone.utc).astimezone(_IST)
                .strftime("%Y-%m-%d %H:%M IST"))
    except Exception:
        return iso_z
START_IST = _to_ist(START)
END_IST = _to_ist(END)

print(f"Period : {START}  →  {END}", flush=True)

CONFIGURED_TIMEOUTS = {
    "Messenger_Low_Latency": 4000,
    "Campaign": 20000,
    "Messenger": 30000,
    "Listing": 30000,
    "Business": 30000,
    "Kontacto": 30000,
    "Survey": 30000,
    "Social": 30000,
    "CustomReport": 20000,
    "Tickz": 30000,
    "Reviews": 30000,
    "GenAi": 20000,
    "Insight": 30000,
    "Doup": 60000,
    "Bizapps": 30000,
    "Nexus": 20000,
    "Authentication": 30000,
    "SpamDetection": 30000,
    "BAM": 30000,
    "Raptor": 30000,
    "BirdAiAgent": 20000,
    "Workflows": 20000,
    "Appointments": 20000,
}

STATUS_LABELS = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    408: "Timeout",
    409: "Conflict",
    422: "Unprocessable",
    429: "Rate Limited",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}

STATUS_COLORS = {
    4: "#e67e22",   # 4xx — orange
    5: "#e74c3c",   # 5xx — red
}

TIMEOUT_CODES = {408, 504}

# ── helpers ──────────────────────────────────────────────────────────────────

def run_query(label, query, api_key, base, start, end):
    print(f"[{label}] Submitting...", flush=True)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "query": query,
        "syntax": "QUERY_SYNTAX_DATAPRIME",
        "startDate": start,
        "endDate": end,
    }
    try:
        r = requests.post(f"{base}/api/v1/dataprime/background-query",
                          headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()
    except Exception as e:
        body = getattr(e, "response", None)
        body_text = body.text[:300] if body is not None else ""
        print(f"[{label}] Submit failed: {e} | response: {body_text}", flush=True)
        return []

    qid = resp.get("queryId")
    if not qid:
        print(f"[{label}] No queryId: {resp}", flush=True)
        return []

    print(f"[{label}] queryId={qid}, polling...", flush=True)
    deadline = time.time() + 360
    while time.time() < deadline:
        time.sleep(5)
        try:
            sr = requests.post(f"{base}/api/v1/dataprime/background-query/status",
                               headers=headers, json={"queryId": qid}, timeout=30)
            sr.raise_for_status()
            if "terminated" in sr.text.lower():
                break
        except Exception as e:
            print(f"[{label}] Poll error: {e}", flush=True)
    else:
        print(f"[{label}] Timed out, cancelling...", flush=True)
        try:
            requests.post(f"{base}/api/v1/dataprime/background-query/cancel",
                          headers=headers, json={"queryId": qid}, timeout=10)
        except Exception:
            pass
        return []

    print(f"[{label}] Fetching results...", flush=True)
    try:
        dr = requests.post(f"{base}/api/v1/dataprime/background-query/data",
                           headers=headers, json={"queryId": qid}, timeout=60)
        dr.raise_for_status()
        raw = dr.text
    except Exception as e:
        print(f"[{label}] Fetch failed: {e}", flush=True)
        return []

    results = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        rows = None
        try:
            rows = obj["response"]["results"]["results"]
        except (KeyError, TypeError):
            pass
        if rows is None:
            rows = obj if isinstance(obj, list) else None
        if rows is None:
            continue
        for row in rows:
            ud = row.get("userData")
            if ud is None:
                results.append(row)
                continue
            if isinstance(ud, str):
                try:
                    ud = json.loads(ud)
                except Exception:
                    continue
            results.append(ud)

    print(f"[{label}] {len(results)} rows", flush=True)
    if results:
        print(f"[{label}] sample keys: {list(results[0].keys())}", flush=True)
    return results


def dp_get(row, *path):
    """
    Fetch a nested field from a DataPrime result row.
    Handles both nested dicts  (row["p"]["accountId"])
    and dot-notation flat keys (row["p.accountId"]).
    """
    # try dot-notation flat key first, e.g. dp_get(r, "p", "accountId")
    flat_key = ".".join(path)
    if flat_key in row:
        return row[flat_key]
    # try nested traversal
    cur = row
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def escape_html(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_ms(ms):
    try:
        ms = float(ms)
        return f"{ms/1000:.1f}s" if ms >= 1000 else f"{int(ms)}ms"
    except Exception:
        return str(ms)


def status_badge(code):
    try:
        code = int(code)
    except Exception:
        return f'<span class="badge badge-gray">{escape_html(str(code))}</span>'
    label = STATUS_LABELS.get(code, "")
    label_str = f"{code} {label}" if label else str(code)
    if code in TIMEOUT_CODES:
        return f'<span class="badge badge-purple">{label_str}</span>'
    elif code >= 500:
        return f'<span class="badge badge-red">{label_str}</span>'
    elif code >= 400:
        return f'<span class="badge badge-orange">{label_str}</span>'
    return f'<span class="badge badge-gray">{label_str}</span>'


def severity_badge(system, avg_rt):
    timeout = CONFIGURED_TIMEOUTS.get(system, 30000)
    try:
        ratio = float(avg_rt) / timeout
    except Exception:
        ratio = 0
    if ratio >= 0.9:
        return '<span class="badge badge-red">CRITICAL</span>'
    elif ratio >= 0.7:
        return '<span class="badge badge-orange">HIGH</span>'
    return '<span class="badge badge-yellow">MEDIUM</span>'


def get_int(row, *keys, default=0):
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    return default


# ── queries ──────────────────────────────────────────────────────────────────

QUERIES = [
    # Q1 — all downstream errors by system + status code
    ("Q1_system_status_breakdown",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.system != null | filter $d.system_status_code >= 400 "
     "| groupby $d.system, $d.system_status_code aggregate count() as cnt, "
     "avg($d.service_response_time) as avg_rt, max($d.service_response_time) as max_rt "
     "| orderby cnt desc"),

    # Q2 — total downstream error count
    ("Q2_total_errors",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.system != null | filter $d.system_status_code >= 400 | count"),

    # Q3 — by system (rolled up)
    ("Q3_by_system",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.system != null | filter $d.system_status_code >= 400 "
     "| groupby $d.system aggregate count() as cnt, avg($d.service_response_time) as avg_rt "
     "| orderby cnt desc"),

    # Q4 — by status code distribution
    ("Q4_by_status_code",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.system != null | filter $d.system_status_code >= 400 "
     "| countby $d.system_status_code desc"),

    # Q5 — top API endpoints per system (all errors)
    ("Q5_api_breakdown",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.system != null | filter $d.system_status_code >= 400 "
     "| redact $d.system_api matching /[0-9]+/ to '${PV}' "
     "| create $d.system_api from splitParts($d.system_api, '?', 1) "
     "| groupby $d.system, $d.system_api, $d.system_status_code aggregate count() as cnt "
     "| orderby cnt desc | limit 100"),

    # Q6 — account breakdown (interceptor — ambassador 500s, which covers all downstream errors)
    ("Q6_account_breakdown",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.logger_name == 'com.birdeye.ambassador.interceptor.AmbassadorRequestInterceptor' "
     "| filter $d.message:string.startsWith('AmbassadorRequestInterceptor-afterCompletion') "
     "| filter $d.message:string.contains('status:500') "
     "| extract $d.message:string into $d.p using regexp(e=/time:(?<time>\\d+)ms status:(?<status>\\d+) "
     "source:(?<source>\\S+) userId:(?<userId>\\S+) bizId:(?<bizId>\\S+) bizName:(?<bizName>.*?) "
     "bizType:(?<bizType>\\S+) x-account-id:(?<accountId>\\S+)/) "
     "| groupby $d.p.accountId, $d.p.bizName aggregate count() as cnt "
     "| orderby cnt desc | limit 100"),

    # Q7 — by request source (ambassador 500s)
    ("Q7_by_source",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.logger_name == 'com.birdeye.ambassador.interceptor.AmbassadorRequestInterceptor' "
     "| filter $d.message:string.startsWith('AmbassadorRequestInterceptor-afterCompletion') "
     "| filter $d.message:string.contains('status:500') "
     "| extract $d.message:string into $d.p using regexp(e=/source:(?<source>\\S+)/) "
     "| countby $d.p.source desc"),

    # Q8 — ambassador endpoints (ambassador 500s)
    ("Q8_ambassador_endpoints",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.logger_name == 'com.birdeye.ambassador.interceptor.AmbassadorRequestInterceptor' "
     "| filter $d.message:string.startsWith('AmbassadorRequestInterceptor-afterCompletion') "
     "| filter $d.message:string.contains('status:500') "
     "| extract $d.message:string into $d.p using regexp(e=/URI:(?<uri>.*?) time:(?<time>\\d+)ms/) "
     "| redact $d.p.uri matching /[0-9]+/ to '${PV}' "
     "| create $d.p.uri from splitParts($d.p.uri, '?', 1) "
     "| countby $d.p.uri desc | limit 50"),

    # Q9 — per-account per-URI (ambassador 500s)
    ("Q9_per_account_uri",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.logger_name == 'com.birdeye.ambassador.interceptor.AmbassadorRequestInterceptor' "
     "| filter $d.message:string.startsWith('AmbassadorRequestInterceptor-afterCompletion') "
     "| filter $d.message:string.contains('status:500') "
     "| extract $d.message:string into $d.p using regexp(e=/URI:(?<uri>.*?) time:(?<time>\\d+)ms "
     "status:(?<status>\\d+) source:(?<source>\\S+) userId:(?<userId>\\S+) bizId:(?<bizId>\\S+) "
     "bizName:(?<bizName>.*?) bizType:(?<bizType>\\S+) x-account-id:(?<accountId>\\S+)/) "
     "| redact $d.p.uri matching /[0-9]+/ to '${PV}' "
     "| create $d.p.uri from splitParts($d.p.uri, '?', 1) "
     "| groupby $d.p.accountId, $d.p.bizName, $d.p.uri aggregate count() as cnt "
     "| orderby cnt desc"),

    # Q10 — near-timeout slow calls (>24s, succeeded)
    ("Q10_near_timeout",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.service_response_time > 24000 "
     "| filter $d.system_status_code < 400 | filter $d.system_status_code > 0 "
     "| redact $d.system_api matching /[0-9]+/ to '${PV}' "
     "| create $d.system_api from splitParts($d.system_api, '?', 1) "
     "| groupby $d.system, $d.system_api aggregate count() as cnt, "
     "max($d.service_response_time) as max_rt"),

    # Q11 — timeout-only breakdown (408/504) by system
    ("Q11_timeout_by_system",
     "source logs | filter $l.subsystemname == 'ambassador-service-java-backend' "
     "| filter $d.system != null "
     "| filter $d.system_status_code == 408 || $d.system_status_code == 504 "
     "| groupby $d.system aggregate count() as cnt, avg($d.service_response_time) as avg_rt "
     "| orderby cnt desc"),
]


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    raw = {}
    for label, query in QUERIES:
        raw[label] = run_query(label, query, API_KEY, BASE, START, END)

    # ── parse results ────────────────────────────────────────────────────────

    # Total errors
    total_errors = 0
    for r in raw["Q2_total_errors"]:
        v = r.get("_count") or r.get("count") or r.get("cnt") or 0
        try:
            total_errors = int(v)
        except Exception:
            pass

    # Status code distribution
    status_dist = {}
    for r in raw["Q4_by_status_code"]:
        code_raw = r.get("system_status_code") or r.get("$d.system_status_code") or r.get("_count_by") or 0
        cnt = get_int(r, "_count", "count", "cnt")
        try:
            code = int(float(code_raw))
        except Exception:
            continue
        status_dist[code] = status_dist.get(code, 0) + cnt
    status_dist_sorted = sorted(status_dist.items(), key=lambda x: x[1], reverse=True)
    total_timeout_events = sum(v for k, v in status_dist.items() if k in TIMEOUT_CODES)
    total_rate_limit_events = status_dist.get(429, 0)
    total_5xx_events = sum(v for k, v in status_dist.items() if 500 <= k < 600)
    total_4xx_events = sum(v for k, v in status_dist.items() if 400 <= k < 500)

    # By system (rolled up)
    system_data = []
    for r in raw["Q3_by_system"]:
        sys_name = r.get("system") or r.get("$d.system") or ""
        cnt = get_int(r, "cnt", "count")
        avg_rt = 0
        try:
            avg_rt = float(r.get("avg_rt") or 0)
        except Exception:
            pass
        if sys_name and cnt >= 1:
            system_data.append({"system": sys_name, "cnt": cnt, "avg_rt": avg_rt})
    system_data.sort(key=lambda x: x["cnt"], reverse=True)

    # System + status code breakdown
    sys_status_map = {}  # system -> {status_code -> {cnt, avg_rt, max_rt}}
    for r in raw["Q1_system_status_breakdown"]:
        sys_name = r.get("system") or r.get("$d.system") or ""
        code_raw = r.get("system_status_code") or r.get("$d.system_status_code") or 0
        cnt = get_int(r, "cnt", "count")
        avg_rt = 0
        max_rt = 0
        try:
            avg_rt = float(r.get("avg_rt") or 0)
            max_rt = float(r.get("max_rt") or 0)
        except Exception:
            pass
        try:
            code = int(float(code_raw))
        except Exception:
            continue
        if not sys_name or cnt < 1:
            continue
        if sys_name not in sys_status_map:
            sys_status_map[sys_name] = {}
        sys_status_map[sys_name][code] = {"cnt": cnt, "avg_rt": avg_rt, "max_rt": max_rt}

    # API breakdown per system (Q5)
    api_data = []
    for r in raw["Q5_api_breakdown"]:
        sys_name = r.get("system") or r.get("$d.system") or ""
        api = r.get("system_api") or r.get("$d.system_api") or ""
        code_raw = r.get("system_status_code") or r.get("$d.system_status_code") or 0
        cnt = get_int(r, "cnt", "count")
        try:
            code = int(float(code_raw))
        except Exception:
            code = 0
        if sys_name and api and cnt >= 2:
            api_data.append({"system": sys_name, "api": api, "code": code, "cnt": cnt})
    api_data.sort(key=lambda x: x["cnt"], reverse=True)

    # Accounts (Q6)
    account_data = []
    for r in raw["Q6_account_breakdown"]:
        acct_id = dp_get(r, "p", "accountId") or r.get("accountId") or ""
        biz_name = dp_get(r, "p", "bizName") or r.get("bizName") or ""
        cnt = get_int(r, "cnt", "count")
        if acct_id and cnt >= 3:
            account_data.append({"accountId": acct_id, "bizName": biz_name, "cnt": cnt})
    account_data.sort(key=lambda x: x["cnt"], reverse=True)

    # Source distribution (Q7) — Q8 now uses countby so field is _count_by / p.source
    source_data = []
    src_total = 0
    for r in raw["Q7_by_source"]:
        src = dp_get(r, "p", "source") or r.get("source") or r.get("_count_by") or ""
        cnt = get_int(r, "_count", "count", "cnt")
        if src:
            source_data.append({"source": src, "cnt": cnt})
            src_total += cnt
    source_data.sort(key=lambda x: x["cnt"], reverse=True)

    # Ambassador endpoints (Q8) — now uses countby, field is p.uri / _count_by
    endpoint_data = []
    for r in raw["Q8_ambassador_endpoints"]:
        uri = dp_get(r, "p", "uri") or r.get("uri") or r.get("_count_by") or ""
        cnt = get_int(r, "_count", "cnt", "count")
        if uri and cnt >= 3:
            endpoint_data.append({"uri": uri, "total": cnt, "by_code": {500: cnt}})
    endpoint_data.sort(key=lambda x: x["total"], reverse=True)

    # Per-account URI (Q9)
    acct_uri_map = {}  # (acct_id, biz_name) -> {uri -> cnt}
    for r in raw["Q9_per_account_uri"]:
        acct_id = dp_get(r, "p", "accountId") or r.get("accountId") or ""
        biz_name = dp_get(r, "p", "bizName") or r.get("bizName") or ""
        uri = dp_get(r, "p", "uri") or r.get("uri") or ""
        cnt = get_int(r, "cnt", "count")
        if not acct_id:
            continue
        key = (acct_id, biz_name)
        if key not in acct_uri_map:
            acct_uri_map[key] = {}
        acct_uri_map[key][uri] = acct_uri_map[key].get(uri, 0) + cnt

    acct_uri_blocks = []
    for (acct_id, biz_name), uri_map in acct_uri_map.items():
        total = sum(uri_map.values())
        if total < 3:
            continue
        uris = [{"uri": u, "total": c} for u, c in uri_map.items() if c >= 2]
        uris.sort(key=lambda x: x["total"], reverse=True)
        acct_uri_blocks.append({"accountId": acct_id, "bizName": biz_name, "total": total, "uris": uris})
    acct_uri_blocks.sort(key=lambda x: x["total"], reverse=True)

    # Near-timeout (Q10)
    near_timeout_data = []
    for r in raw["Q10_near_timeout"]:
        sys_name = r.get("system") or r.get("$d.system") or ""
        api = r.get("system_api") or r.get("$d.system_api") or ""
        cnt = get_int(r, "cnt", "count")
        max_rt = 0
        try:
            max_rt = float(r.get("max_rt") or 0)
        except Exception:
            pass
        if cnt >= 2:
            near_timeout_data.append({"system": sys_name, "api": api, "cnt": cnt, "max_rt": max_rt})
    near_timeout_data.sort(key=lambda x: x["cnt"], reverse=True)

    # Timeout by system (Q11)
    timeout_system_data = []
    for r in raw["Q11_timeout_by_system"]:
        sys_name = r.get("system") or r.get("$d.system") or ""
        cnt = get_int(r, "cnt", "count")
        avg_rt = 0
        try:
            avg_rt = float(r.get("avg_rt") or 0)
        except Exception:
            pass
        if sys_name and cnt >= 1:
            timeout_system_data.append({"system": sys_name, "cnt": cnt, "avg_rt": avg_rt})
    timeout_system_data.sort(key=lambda x: x["cnt"], reverse=True)

    # ── derived stats ────────────────────────────────────────────────────────
    avg_per_hour     = round(total_errors / 24, 1) if total_errors else 0
    systems_affected = len(system_data)
    accounts_affected = len(account_data)
    endpoints_affected = len(endpoint_data)

    top_system   = system_data[0]["system"] if system_data else "N/A"
    top_system_cnt = system_data[0]["cnt"] if system_data else 0
    top_account  = account_data[0]["bizName"] if account_data else "N/A"
    top_account_cnt = account_data[0]["cnt"] if account_data else 0

    # ── key findings ─────────────────────────────────────────────────────────
    findings = []
    if total_errors:
        findings.append(
            f"<strong>{total_errors:,}</strong> downstream errors recorded in the last 24 hours "
            f"— <strong>{avg_per_hour}</strong> per hour on average.")
    if total_5xx_events and total_errors:
        pct = round(total_5xx_events / total_errors * 100)
        findings.append(
            f"<strong>{total_5xx_events:,}</strong> 5xx server errors ({pct}% of total) — "
            f"indicate downstream service instability.")
    if total_timeout_events:
        findings.append(
            f"<strong>{total_timeout_events:,}</strong> timeout events (408/504) — "
            f"requests that exceeded configured downstream limits.")
    if total_rate_limit_events:
        findings.append(
            f"<strong>{total_rate_limit_events:,}</strong> rate-limit (429) responses — "
            f"review throttling policies or introduce request queuing.")
    if system_data:
        pct = round(top_system_cnt / total_errors * 100) if total_errors else 0
        findings.append(
            f"<strong>{escape_html(top_system)}</strong> is the top error source with "
            f"<strong>{top_system_cnt:,}</strong> errors ({pct}% of total).")
    if account_data:
        findings.append(
            f"<strong>{escape_html(top_account)}</strong> is the most impacted account "
            f"with <strong>{top_account_cnt:,}</strong> errors.")

    # ── recommendations ──────────────────────────────────────────────────────
    recommendations = []
    if system_data:
        recommendations.append(
            f"Prioritise investigation of <strong>{escape_html(top_system)}</strong> — "
            f"it accounts for the largest share of errors and may need capacity or config review.")
    if total_rate_limit_events > 0:
        recommendations.append(
            "Implement exponential back-off and request queuing for rate-limited (429) calls "
            "to avoid thundering-herd retries amplifying the problem.")
    if total_timeout_events > 0:
        recommendations.append(
            "Review timeout thresholds for high-timeout systems — consider circuit-breaker "
            "patterns to fail fast instead of holding threads.")
    if near_timeout_data:
        recommendations.append(
            f"{len(near_timeout_data)} endpoints are near the timeout threshold (&gt;80% of limit) "
            "— add caching or query optimisation before they tip over.")
    if account_data:
        recommendations.append(
            f"Contact top affected accounts (starting with <strong>{escape_html(top_account)}</strong>) "
            "to understand usage patterns driving repeated errors.")
    recommendations.append(
        "Set up per-error-code alerting (5xx spike, 429 burst) so on-call is paged before "
        "errors accumulate to report-level volumes.")

    # ── CSS ──────────────────────────────────────────────────────────────────
    css = """
* {margin:0;padding:0;box-sizing:border-box}
body {font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f5f5f5;color:#333;padding:20px;font-size:13px}
.container {max-width:1400px;margin:0 auto}
h1 {color:#1a1a2e;margin-bottom:5px;font-size:22px}
.subtitle {color:#666;margin-bottom:20px;font-size:13px}
.card {background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,0.1)}
.card h2 {color:#1a1a2e;font-size:16px;margin-bottom:12px;border-bottom:2px solid #e74c3c;padding-bottom:8px}
table {width:100%;border-collapse:collapse;font-size:12px}
th {background:#2c3e50;color:#fff;padding:6px 8px;text-align:left;font-weight:500;position:sticky;top:0}
td {padding:5px 8px;border-bottom:1px solid #eee}
tr:hover td {background:#f8f9fa}
.badge {display:inline-block;padding:2px 8px;border-radius:12px;font-size:10px;font-weight:600}
.badge-red    {background:#fde8e8;color:#e74c3c}
.badge-orange {background:#fef3e2;color:#e67e22}
.badge-yellow {background:#fef9e7;color:#f39c12}
.badge-purple {background:#f3e8fd;color:#8e44ad}
.badge-gray   {background:#f0f0f0;color:#666}
.stat-grid {display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:16px}
.stat-grid-4 {display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}
.stat {text-align:center;padding:12px;background:#f8f9fa;border-radius:8px}
.stat .num {font-size:26px;font-weight:700;color:#e74c3c}
.stat .num.orange {color:#e67e22}
.stat .num.purple {color:#8e44ad}
.stat .num.blue   {color:#2980b9}
.stat .label {font-size:11px;color:#666;margin-top:4px}
.mono {font-family:monospace;font-size:11px;word-break:break-all}
.scroll-table {max-height:500px;overflow-y:auto}
/* expandable account cards */
.acct-block {background:#f8f9fa;border-radius:6px;margin-bottom:10px;border-left:3px solid #3498db;overflow:hidden}
.acct-block summary {display:flex;justify-content:space-between;align-items:center;padding:12px;cursor:pointer;list-style:none;user-select:none}
.acct-block summary::-webkit-details-marker {display:none}
.acct-block summary:hover {background:#eef4fb}
.acct-block[open] summary {border-bottom:1px solid #dde8f5;background:#e8f1fb}
.acct-toggle {font-size:11px;color:#3498db;margin-left:8px;transition:transform 0.2s}
.acct-block[open] .acct-toggle {transform:rotate(90deg)}
.acct-header-left {display:flex;align-items:center;gap:10px}
.acct-name {font-weight:700;font-size:13px;color:#2c3e50}
.acct-id   {font-size:11px;color:#888;font-family:monospace}
.acct-count {font-weight:700;color:#e74c3c;font-size:14px;white-space:nowrap}
.acct-body {padding:10px 12px}
.api-row {display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid #eee;font-size:11px;gap:8px}
.api-row:last-child {border-bottom:none}
.api-uri {font-family:monospace;color:#555;flex:1;min-width:0;word-break:break-all}
.api-badges {display:flex;gap:4px;flex-wrap:wrap;justify-content:flex-end}
.api-cnt {font-weight:600;min-width:30px;text-align:right}
/* status code pill table */
.code-pill {display:inline-block;padding:1px 6px;border-radius:10px;font-size:10px;font-weight:600;margin:1px}
.code-5xx {background:#fde8e8;color:#c0392b}
.code-4xx {background:#fef3e2;color:#c0392b}
.code-408, .code-504 {background:#f3e8fd;color:#6c3483}
.code-429 {background:#fef9e7;color:#7d6608}
@media print {
  body{background:#fff;font-size:11px}
  .card{box-shadow:none;border:1px solid #ddd;page-break-inside:avoid}
  .scroll-table{max-height:none;overflow:visible}
  .acct-block[open] .acct-body, .acct-body {display:block!important}
}
"""

    # ── build sections ───────────────────────────────────────────────────────
    generated_ts = datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S IST")

    # Stat grid
    stat_grid_html = f"""
<div class="stat-grid">
  <div class="stat"><div class="num">{total_errors:,}</div><div class="label">Total Errors (4xx+5xx)</div></div>
  <div class="stat"><div class="num orange">{total_5xx_events:,}</div><div class="label">5xx Server Errors</div></div>
  <div class="stat"><div class="num purple">{total_timeout_events:,}</div><div class="label">Timeouts (408/504)</div></div>
  <div class="stat"><div class="num orange">{total_rate_limit_events:,}</div><div class="label">Rate Limited (429)</div></div>
  <div class="stat"><div class="num">{avg_per_hour}</div><div class="label">Avg Errors / Hour</div></div>
</div>
<div class="stat-grid-4">
  <div class="stat"><div class="num blue">{systems_affected}</div><div class="label">Systems Affected</div></div>
  <div class="stat"><div class="num blue">{accounts_affected}</div><div class="label">Accounts Affected (&ge;3 errors)</div></div>
  <div class="stat"><div class="num blue">{endpoints_affected}</div><div class="label">Ambassador Endpoints (&ge;3 errors)</div></div>
  <div class="stat"><div class="num orange">{len(near_timeout_data)}</div><div class="label">Near-Timeout Endpoints</div></div>
</div>"""

    # Status code distribution
    code_rows_html = ""
    for code, cnt in status_dist_sorted:
        pct = round(cnt / total_errors * 100, 1) if total_errors else 0
        code_rows_html += f"<tr><td>{status_badge(code)}</td><td>{cnt:,}</td><td>{pct}%</td></tr>"

    # System breakdown table (with per-code pills)
    sys_rows_html = ""
    for sd in system_data:
        display_name = "Business (Authentication)" if sd["system"] == "Authentication" else sd["system"]
        pct = round(sd["cnt"] / total_errors * 100, 1) if total_errors else 0
        codes = sys_status_map.get(sd["system"], {})
        pills = ""
        for code, info in sorted(codes.items(), key=lambda x: x[1]["cnt"], reverse=True):
            css_cls = "code-408" if code == 408 else ("code-504" if code == 504 else
                      ("code-429" if code == 429 else
                      ("code-5xx" if code >= 500 else "code-4xx")))
            pills += f'<span class="code-pill {css_cls}">{code}: {info["cnt"]:,}</span>'
        sys_rows_html += f"""<tr>
<td>{escape_html(display_name)}</td>
<td>{sd['cnt']:,}</td>
<td>{pct}%</td>
<td>{pills}</td>
</tr>"""

    # API breakdown table
    api_rows_html = ""
    for i, ad in enumerate(api_data[:50], 1):
        api_rows_html += f"""<tr>
<td>{i}</td>
<td>{escape_html(ad['system'])}</td>
<td class="mono">{escape_html(ad['api'])}</td>
<td>{status_badge(ad['code'])}</td>
<td>{ad['cnt']:,}</td>
</tr>"""

    # Ambassador endpoint table
    ep_rows_html = ""
    for i, ep in enumerate(endpoint_data, 1):
        pills = ""
        for code, cnt in sorted(ep["by_code"].items(), key=lambda x: x[1], reverse=True):
            css_cls = "code-408" if code == 408 else ("code-504" if code == 504 else
                      ("code-429" if code == 429 else
                      ("code-5xx" if code >= 500 else "code-4xx")))
            pills += f'<span class="code-pill {css_cls}">{code}: {cnt:,}</span>'
        ep_rows_html += f"""<tr>
<td>{i}</td>
<td class="mono">{escape_html(ep['uri'])}</td>
<td>{ep['total']:,}</td>
<td>{pills}</td>
</tr>"""

    # Accounts table
    acct_rows_html = ""
    for i, ac in enumerate(account_data, 1):
        acct_rows_html += f"""<tr>
<td>{i}</td>
<td class="mono">{escape_html(ac['accountId'])}</td>
<td>{escape_html(ac['bizName'])}</td>
<td>{ac['cnt']:,}</td>
</tr>"""

    # Per-account URI expandable cards
    acct_cards_html = ""
    for blk in acct_uri_blocks:
        uri_rows = ""
        for uri_info in blk["uris"]:
            uri_rows += f"""<div class="api-row">
<span class="api-uri">{escape_html(uri_info['uri'])}</span>
<span class="api-cnt">{uri_info['total']:,}</span>
</div>"""
        acct_cards_html += f"""<details class="acct-block">
<summary>
  <div class="acct-header-left">
    <span class="acct-name">{escape_html(blk['bizName'] or 'Unknown')}</span>
    <span class="acct-id">{escape_html(blk['accountId'])}</span>
  </div>
  <div style="display:flex;align-items:center;gap:6px">
    <span class="acct-count">{blk['total']:,} errors</span>
    <span class="acct-toggle">&#9654;</span>
  </div>
</summary>
<div class="acct-body">{uri_rows}</div>
</details>"""

    # Source distribution
    src_rows_html = ""
    for sd in source_data:
        pct = round(sd["cnt"] / src_total * 100, 1) if src_total else 0
        src_rows_html += f"<tr><td>{escape_html(sd['source'])}</td><td>{sd['cnt']:,}</td><td>{pct}%</td></tr>"

    # Timeout breakdown by system
    timeout_sys_rows = ""
    for sd in timeout_system_data:
        display_name = "Business (Authentication)" if sd["system"] == "Authentication" else sd["system"]
        timeout_cfg = CONFIGURED_TIMEOUTS.get(sd["system"], 30000)
        badge = severity_badge(sd["system"], sd["avg_rt"])
        pct = round(sd["cnt"] / total_timeout_events * 100, 1) if total_timeout_events else 0
        timeout_sys_rows += f"""<tr>
<td>{escape_html(display_name)}</td>
<td>{sd['cnt']:,}</td>
<td>{pct}%</td>
<td>{fmt_ms(timeout_cfg)}</td>
<td>{badge}</td>
</tr>"""

    # Near-timeout table
    near_rows_html = ""
    for nd in near_timeout_data:
        timeout_cfg = CONFIGURED_TIMEOUTS.get(nd["system"], 30000)
        risk_pct = round(nd["max_rt"] / timeout_cfg * 100, 1) if timeout_cfg else 0
        near_rows_html += f"""<tr>
<td>{escape_html(nd['system'])}</td>
<td class="mono">{escape_html(nd['api'])}</td>
<td>{nd['cnt']:,}</td>
<td>{fmt_ms(timeout_cfg)}</td>
<td>{risk_pct}%</td>
</tr>"""

    # ── assemble HTML ────────────────────────────────────────────────────────
    findings_html = "".join(f"<li style='margin-bottom:6px'>{f}</li>" for f in findings)
    recs_html     = "".join(f"<li style='margin-bottom:6px'>{r}</li>" for r in recommendations)

    def empty_row(cols, msg="No data"):
        return f'<tr><td colspan="{cols}" style="text-align:center;color:#999">{msg}</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ambassador Error Analysis — Daily Report</title>
<style>{css}</style>
</head>
<body>
<div class="container">

<h1>Ambassador Error Analysis &mdash; Daily Report</h1>
<div class="subtitle">Period: {START_IST} &mdash; {END_IST} ({WINDOW_LABEL}) &nbsp;|&nbsp; Service: ambassador-service-java-backend &nbsp;|&nbsp; Generated: {generated_ts}</div>

{stat_grid_html}

<div class="card">
<h2>Key Findings</h2>
<ul style="padding-left:18px;line-height:1.8">{findings_html}</ul>
</div>

<!-- 1. Status Code Distribution -->
<div class="card">
<h2>Error Distribution by Status Code</h2>
<div class="scroll-table">
<table>
<thead><tr><th>Status Code</th><th>Count</th><th>% of Total</th></tr></thead>
<tbody>{code_rows_html or empty_row(3)}</tbody>
</table>
</div>
</div>

<!-- 2. By System -->
<div class="card">
<h2>Error Breakdown by Downstream System</h2>
<div class="scroll-table">
<table>
<thead><tr><th>System</th><th>Total Errors</th><th>% of Total</th><th>Error Codes</th></tr></thead>
<tbody>{sys_rows_html or empty_row(4)}</tbody>
</table>
</div>
</div>

<!-- 3. API Breakdown by System -->
<div class="card">
<h2>Top Downstream API Errors by System</h2>
<div class="scroll-table">
<table>
<thead><tr><th>#</th><th>System</th><th>API Endpoint</th><th>Status Code</th><th>Count</th></tr></thead>
<tbody>{api_rows_html or empty_row(5)}</tbody>
</table>
</div>
</div>

<!-- 4. Ambassador Endpoints -->
<div class="card">
<h2>Top Ambassador Endpoints by Error Count</h2>
<div class="scroll-table">
<table>
<thead><tr><th>#</th><th>Ambassador Endpoint</th><th>Total Errors</th><th>Error Codes</th></tr></thead>
<tbody>{ep_rows_html or empty_row(4, "No endpoints with &ge;3 errors")}</tbody>
</table>
</div>
</div>

<!-- 5. Accounts Affected -->
<div class="card">
<h2>Accounts Affected by Errors</h2>
<div class="scroll-table">
<table>
<thead><tr><th>#</th><th>Account ID</th><th>Account Name</th><th>Total Errors</th></tr></thead>
<tbody>{acct_rows_html or empty_row(4, "No accounts with &ge;3 errors")}</tbody>
</table>
</div>
</div>

<!-- 6. Per-Account URI Breakdown (expandable) -->
<div class="card">
<h2>Error Breakdown by Account &amp; Endpoint</h2>
{acct_cards_html or '<p style="color:#999;text-align:center">No accounts with &ge;3 errors found</p>'}
</div>

<!-- 7. Source Distribution -->
<div class="card">
<h2>Error Distribution by Request Source</h2>
<div class="scroll-table">
<table>
<thead><tr><th>Source</th><th>Count</th><th>Percentage</th></tr></thead>
<tbody>{src_rows_html or empty_row(3)}</tbody>
</table>
</div>
</div>

<!-- 8. Timeout Deep-Dive -->
<div class="card">
<h2>Timeout Deep-Dive (408 / 504)</h2>
<div class="scroll-table">
<table>
<thead><tr><th>System</th><th>Timeout Count</th><th>% of Timeouts</th><th>Configured Timeout</th><th>Severity</th></tr></thead>
<tbody>{timeout_sys_rows or empty_row(5, "No timeout errors found")}</tbody>
</table>
</div>
</div>

<!-- 9. Near-Timeout Calls -->
<div class="card">
<h2>Near-Timeout Calls (&gt;80% of Configured Limit, Still Succeeded)</h2>
<div class="scroll-table">
<table>
<thead><tr><th>System</th><th>API Endpoint</th><th>Slow Calls</th><th>Configured Timeout</th><th>Risk %</th></tr></thead>
<tbody>{near_rows_html or empty_row(5, "No near-timeout calls found")}</tbody>
</table>
</div>
</div>

<!-- 10. Recommendations -->
<div class="card">
<h2>Recommendations</h2>
<ul style="padding-left:18px;line-height:1.8">{recs_html}</ul>
</div>

</div>
</body>
</html>"""

    os.makedirs("reports", exist_ok=True)
    out_path = f"reports/ambassador-error-analysis-daily-{TODAY}.html"
    with open(out_path, "w") as f:
        f.write(html)

    print(f"\nReport: {out_path}", flush=True)
    print(f"Total errors : {total_errors:,}", flush=True)
    print(f"  5xx        : {total_5xx_events:,}", flush=True)
    print(f"  4xx        : {total_4xx_events:,}", flush=True)
    print(f"  Timeouts   : {total_timeout_events:,}", flush=True)
    print(f"  Rate-limit : {total_rate_limit_events:,}", flush=True)
    print(f"Top systems  : {[s['system'] for s in system_data[:3]]}", flush=True)


if __name__ == "__main__":
    main()
