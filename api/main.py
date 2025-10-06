import os, sqlite3, time
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import httpx
from dotenv import load_dotenv
from datetime import datetime, timezone
import asyncio
import logging, traceback
import json

logger = logging.getLogger("uvicorn.error")

load_dotenv()
API_KEY = os.getenv("API_KEY")
FMCSA_WEBKEY = os.getenv("FMCSA_WEBKEY")
DB_PATH = os.getenv("DB_PATH", "./data/loads_challenge.sqlite")

def require_key(x_api_key: Optional[str]):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def conn():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="Database not found")
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def init_aux_tables():
    c = conn()
    c.execute("""
    CREATE TABLE IF NOT EXISTS calls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mc_number TEXT,
        dot_number TEXT,
        carrier_name TEXT,
        selected_load_id TEXT,
        agreed_rate REAL,
        rounds INTEGER,
        outcome TEXT,
        sentiment TEXT,
        fmcsadata TEXT,
        transcript_url TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

try:
    init_aux_tables()
except Exception:
    pass

app = FastAPI(title="Inbound Carrier Sales API")

# -------------------- models ------------------------
class OfferLog(BaseModel):
    load_id: str
    mc_number: str
    carrier_offer: float
    notes: Optional[str] = None

class CallReport(BaseModel):
    mc_number: Optional[str] = None
    dot_number: Optional[str] = None
    carrier_name: Optional[str] = None
    selected_load_id: Optional[str] = None
    agreed_rate: Optional[float] = None
    rounds: Optional[int] = None
    outcome: str
    sentiment: Optional[str] = "neutral"
    fmcsadata: Optional[str] = None
    transcript_url: Optional[str] = None

class NegInput(BaseModel):
    loadboard_rate: float
    carrier_offer: float
    round_index: int
    miles: Optional[int] = None
    equipment_type: Optional[str] = None

# -------------------- FMCSA verification -------------
_FMCSA_CACHE = {}  # DOT -> (expiry, payload)
TTL = 60 * 60 * 12

def _first_content(payload: dict) -> dict:
    """
    FMCSA sometimes returns {"content":[{...}]} and sometimes {"content":{...}}.
    This normalizes to the first dict or {}.
    """
    content = payload.get("content")
    if isinstance(content, list):
        return content[0] if content else {}
    if isinstance(content, dict):
        return content
    return {}

async def _fmcsa_get(url: str, params: dict | None = None):
    if not FMCSA_WEBKEY:
        raise HTTPException(status_code=502, detail="FMCSA webKey not configured (set FMCSA_WEBKEY).")
    q = dict(params or {})
    q["webKey"] = FMCSA_WEBKEY
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(url, params=q, headers={"Accept": "application/json"})
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="FMCSA timed out")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"FMCSA request error: {e}")

    code = r.status_code
    if 200 <= code < 300:
        try:
            return r.json()
        except ValueError:
            raise HTTPException(status_code=502, detail="FMCSA returned invalid JSON")

    if code in (400, 404):
        raise HTTPException(status_code=404, detail="FMCSA record not found")
    if code in (401, 403):
        raise HTTPException(status_code=502, detail="FMCSA auth error (check FMCSA_WEBKEY)")
    if code == 429:
        raise HTTPException(status_code=503, detail="FMCSA rate limited")
    if 500 <= code < 600:
        raise HTTPException(status_code=502, detail=f"FMCSA server error ({code})")
    raise HTTPException(status_code=502, detail=f"FMCSA unexpected status ({code})")


@app.get("/api/v1/carriers/find")
async def carriers_find(mc: Optional[str] = None,
                        dot: Optional[str] = None,
                        mock: Optional[bool] = False,
                        x_api_key: Optional[str] = Header(None),
                        authorization: Optional[str] = Header(None)):
    token = x_api_key or (authorization.split(None,1)[1] if authorization and authorization.lower().startswith("bearer ") else None)
    require_key(token)

    if mock:
        return {
            "statusCode": 200,
            "body": {"carrier": {
                "carrier_id": "DOT-0000000",
                "carrier_name": "Mock Transport LLC",
                "status": "Active",
                "dot_number": "0000000",
                "mc_number": mc or "000000",
                "contacts": [{"phone": "555-0100", "type": "dispatch"}],
                "fmcsadata": {"allowToOperate": "Y", "outOfService": "N", "authority": {"motorCarrier": "Common"}}
            }}
        }

    if not (mc or dot):
        raise HTTPException(status_code=400, detail="mc or dot required")

    try:
        base = "https://mobile.fmcsa.dot.gov/qc/services"

        # MC -> DOT
        if not dot and mc:
            res = await _fmcsa_get(f"{base}/carriers/docket-number/{mc}")
            first = _first_content(res)
            if not first:
                raise HTTPException(status_code=404, detail="MC not found")
            carrier_node = first.get("carrier") or first
            dot_val = carrier_node.get("dotNumber") or first.get("dotNumber")
            if not dot_val:
                raise HTTPException(status_code=404, detail="MC found but DOT missing")
            dot = str(dot_val)

        # Snapshot (with simple cache)
        now = time.time()
        if dot in _FMCSA_CACHE and _FMCSA_CACHE[dot][0] > now:
            snap = _FMCSA_CACHE[dot][1]
        else:
            snap = await _fmcsa_get(f"{base}/carriers/{dot}")
            _FMCSA_CACHE[dot] = (now + TTL, snap)

        # Authority (best-effort)
        try:
            authority = await _fmcsa_get(f"{base}/carriers/{dot}/authority")
        except HTTPException as e:
            authority = {"note": f"authority lookup skipped: {e.detail}"}

        # Normalize shapes: payloads sometimes put data under content[0].carrier
        snap_first = _first_content(snap)
        carr = snap_first.get("carrier") or snap_first

        allowed = (str(carr.get("allowedToOperate") or carr.get("allowToOperate") or "").upper() == "Y")
        out_of_service = (str(carr.get("outOfService") or "").upper() == "Y")
        status = "Active" if (allowed and not out_of_service) else "Ineligible"

        result = {
            "carrier_id": f"DOT-{dot}",
            "carrier_name": carr.get("legalName") or carr.get("dbaName") or "Unknown",
            "status": status,
            "dot_number": str(dot),
            "mc_number": mc or carr.get("mcNumber"),
            "contacts": [{"phone": carr.get("telephone", ""), "type": "dispatch"}],
            "fmcsadata": {
                "allowToOperate": carr.get("allowedToOperate") or carr.get("allowToOperate"),
                "outOfService": carr.get("outOfService"),
                "authority": authority,
            },
        }
        return {"statusCode": 200, "body": {"carrier": result}}

    except HTTPException:
        raise  # let clean 4xx/5xx through
    except Exception as e:
        # surface the actual reason instead of a blank 500
        logger.exception("FMCSA handler error")
        raise HTTPException(
            status_code=502, 
            detail=f"FMCSA handler error: {type(e).__name__}: {e}"
        )

# -------------------- loads search (matches schema) ---
@app.get("/api/v1/loads/search")
def loads_search(origin: Optional[str] = Query(None, description="Exact string like 'Dallas, TX'"),
                 destination: Optional[str] = Query(None, description="Exact string like 'Atlanta, GA'"),
                 equipment_type: Optional[str] = Query(None, description="Dry Van | Reefer | Flatbed | Power Only"),
                 pickup_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
                 x_api_key: Optional[str] = Header(None)):
    """
    Returns up to 3 loads from table `loads` with columns:
    load_id, origin, destination, pickup_datetime, delivery_datetime, equipment_type,
    loadboard_rate, notes, weight, commodity_type, num_of_pieces, miles, dimensions
    """
    require_key(x_api_key)
    q = "SELECT * FROM loads WHERE 1=1"
    params: List[str] = []
    if origin:
        q += " AND origin = ?"; params.append(origin)
    if destination:
        q += " AND destination = ?"; params.append(destination)
    if equipment_type:
        q += " AND equipment_type = ?"; params.append(equipment_type)
    if pickup_date:
        # pickup_datetime is stored as ISO text; SQLite date() works on that
        q += " AND date(pickup_datetime) = date(?)"; params.append(pickup_date)
    q += " LIMIT 3"
    with conn() as c:
        rows = c.execute(q, params).fetchall()
    return {"statusCode": 200, "body": {"loads": [dict(r) for r in rows]}}

@app.get("/api/v1/loads/{load_id}")
def load_by_id(load_id: str, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    with conn() as c:
        r = c.execute("SELECT * FROM loads WHERE load_id = ?", (load_id,)).fetchone()
    if not r:
        raise HTTPException(404, "Not found")
    return {"statusCode": 200, "body": {"load": dict(r)}}

# -------------------- negotiation ---------------------
def _floor_rate(list_rate: float, miles: Optional[int], equip: Optional[str]) -> float:
    base = 0.93 * list_rate
    if equip in {"Reefer", "Flatbed"}:
        base = max(base, 0.94 * list_rate)
    if miles and miles > 1200:
        base = min(base, 0.92 * list_rate)  
    return round(base, 2)

@app.post("/api/v1/negotiate")
def negotiate(payload: NegInput, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    f = _floor_rate(payload.loadboard_rate, payload.miles, payload.equipment_type)

    if payload.round_index <= 1:
        ceiling = max(f, round(0.98 * payload.loadboard_rate, 2))
    elif payload.round_index == 2:
        ceiling = max(f, round(0.95 * payload.loadboard_rate, 2))
    else:
        ceiling = f  

    if payload.carrier_offer <= ceiling:
        return {
            "decision": "accept",
            "counter_offer": float(payload.carrier_offer),  # accepted rate
            "reason": "carrier met or beat our ceiling"
        }

    if payload.round_index >= 3:
        return {"decision": "reject", "counter_offer": None, "reason": "exceeded rounds / above ceiling"}

    return {"decision": "counter", "counter_offer": ceiling, "reason": "above ceiling this round"}


# -------------------- post-call & metrics --------------
@app.post("/api/v1/call-reports", status_code=201)
def call_reports(report: CallReport, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    with conn() as c:
        c.execute("""
        INSERT INTO calls (mc_number, dot_number, carrier_name, selected_load_id,
                           agreed_rate, rounds, outcome, sentiment, fmcsadata, transcript_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (report.mc_number, report.dot_number, report.carrier_name, report.selected_load_id,
              report.agreed_rate, report.rounds, report.outcome, report.sentiment,
              report.fmcsadata, report.transcript_url))
    return {"ok": True}

@app.get("/api/v1/metrics/summary")
def metrics_summary(x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    with conn() as c:
        total = c.execute("SELECT COUNT(*) FROM calls").fetchone()[0]
        won = c.execute("SELECT COUNT(*) FROM calls WHERE outcome='deal_won'").fetchone()[0]
        rounds = c.execute("SELECT COALESCE(AVG(rounds),0) FROM calls").fetchone()[0]
        delta = c.execute("""
            SELECT COALESCE(AVG(l.loadboard_rate - COALESCE(c.agreed_rate, l.loadboard_rate)),0)
            FROM calls c JOIN loads l ON l.load_id = c.selected_load_id
            WHERE c.agreed_rate IS NOT NULL
        """).fetchone()[0]
    return {
        "calls": total,
        "win_rate": (won / total if total else 0),
        "avg_rounds": rounds,
        "avg_delta_to_list": delta
    }

# -------------------- HR Metrics + Dashboard --------------------

import json
from statistics import mean
from datetime import datetime
from typing import Optional

HR_API_KEY = os.getenv("HR_API_KEY")
HR_ORG_ID = os.getenv("HR_ORG_ID")
HR_USE_CASE_ID = os.getenv("HR_USE_CASE_ID")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN")


def _hr_headers():
    if not (HR_API_KEY and HR_ORG_ID):
        raise HTTPException(status_code=500, detail="HappyRobot credentials missing (HR_API_KEY / HR_ORG_ID).")
    return {
        "authorization": f"Bearer {HR_API_KEY}",
        "x-organization-id": HR_ORG_ID,
    }


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # HappyRobot uses Z suffix; normalize for fromisoformat
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _first_quote_timestamp(run: dict) -> Optional[datetime]:
    """
    Find the earliest time the agent produced a quote/counter (negotiate).
    We look in multiple places because HappyRobot payloads can vary.
    """
    events = run.get("events", [])
    messages = run.get("messages", [])

    # 1) "intermediate" negotiate_http
    for ev in events:
        inter = ev.get("intermediate") or {}
        if (inter.get("name") or "").lower() == "negotiate_http":
            ts = _parse_iso(ev.get("timestamp"))
            if ts:
                return ts

    # 2) top-level negotiate_http OR decision+counter_offer
    for ev in events:
        name = (ev.get("name") or "").lower()
        if name == "negotiate_http" or ("decision" in ev and "counter_offer" in ev):
            ts = _parse_iso(ev.get("timestamp"))
            if ts:
                return ts

    # 3) messages tool_calls showing negotiate
    for m in messages:
        t = _parse_iso(m.get("timestamp"))
        for tc in (m.get("tool_calls") or []):
            fn = ((tc.get("function") or {}).get("name") or "").lower()
            if fn in ("negotiate", "negotiate_http"):
                if t:
                    return t

    # 4) AI action with negotiate-ish event_name
    for ev in events:
        en = (ev.get("event_name") or "").lower()
        if "negotiate" in en:
            ts = _parse_iso(ev.get("timestamp"))
            if ts:
                return ts

    return None


async def _hr_fetch_runs_list(limit: int = 10):
    """Returns a list of recent runs (light payload)."""
    url = "https://platform.happyrobot.ai/api/v1/runs"
    params = {"use_case_id": HR_USE_CASE_ID, "limit": limit}
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(url, params=params, headers=_hr_headers())
        r.raise_for_status()
        return r.json()  # array of runs (no events/messages)


async def _hr_fetch_run_detail(run_id: str):
    """Returns a single run with events/messages detail."""
    url = f"https://platform.happyrobot.ai/api/v1/runs/{run_id}"
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(url, headers=_hr_headers())
        r.raise_for_status()
        return r.json()


def _db_summary():
    """Re-run the same SQL as /api/v1/metrics/summary to avoid HTTP calls to ourselves."""
    with conn() as c:
        total = c.execute("SELECT COUNT(*) FROM calls").fetchone()[0]
        won = c.execute("SELECT COUNT(*) FROM calls WHERE outcome='deal_won'").fetchone()[0]
        rounds = c.execute("SELECT COALESCE(AVG(rounds),0) FROM calls").fetchone()[0]
        delta = c.execute(
            """
            SELECT COALESCE(AVG(l.loadboard_rate - COALESCE(c.agreed_rate, l.loadboard_rate)),0)
            FROM calls c JOIN loads l ON l.load_id = c.selected_load_id
            WHERE c.agreed_rate IS NOT NULL
            """
        ).fetchone()[0]
    return {
        "calls": total,
        "win_rate": (won / total if total else 0),
        "avg_rounds": rounds or 0,
        "avg_delta_to_list": delta or 0,
    }


@app.get("/dashboard/hr/metrics", response_class=JSONResponse)
async def hr_metrics(token: Optional[str] = Query(None)):
    # Simple token gate for dashboard/metrics
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Pull last N runs, then enrich each with detail (events/messages)
        runs_list = await _hr_fetch_runs_list(limit=10)
        detailed = []
        for r in runs_list:
            rid = r.get("id")
            if not rid:
                continue
            try:
                detailed.append(await _hr_fetch_run_detail(rid))
            except httpx.HTTPError:
                # Skip individual failures; continue best-effort
                continue
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"HappyRobot API error: {e}")

    classification_counts: dict[str, int] = {}
    handle_times: list[float] = []
    ttfq_times: list[float] = []
    sample_rows: list[dict] = []

    for d in detailed:
        run_id = d.get("id")
        events = d.get("events", [])

        # --- Classification (from AI -> Classify output)
        classification = None
        for ev in events:
            if (
                ev.get("type") == "action"
                and ev.get("integration_name") == "AI"
                and ev.get("event_name") == "Classify"
            ):
                out = ev.get("output") or {}
                resp = out.get("response") or {}
                classification = resp.get("classification")
                if classification:
                    classification_counts[classification] = classification_counts.get(classification, 0) + 1

        # --- Handle time (prefer session.duration)
        handle_s: Optional[float] = None
        session_start: Optional[datetime] = None
        completed_at = d.get("completed_at")

        for ev in events:
            if ev.get("type") == "session":
                dur = ev.get("duration")
                if isinstance(dur, (int, float)):
                    handle_s = float(dur)
                session_start = _parse_iso(ev.get("timestamp")) or session_start

        if handle_s is None:
            # Fallback: difference between run timestamp and completed_at
            t0 = _parse_iso(d.get("timestamp"))
            t1 = _parse_iso(completed_at)
            if t0 and t1:
                handle_s = max(0.0, (t1 - t0).total_seconds())
        if handle_s is not None:
            handle_times.append(handle_s)

        # If no explicit session_start, fallback to run timestamp
        if session_start is None:
            session_start = _parse_iso(d.get("timestamp"))

        # --- Time to First Quote: robust detection
        first_quote_ts = _first_quote_timestamp(d)
        if first_quote_ts and session_start:
            ttfq_times.append(max(0.0, (first_quote_ts - session_start).total_seconds()))

        # --- Row for the table
        sample_rows.append(
            {
                "run_id": run_id,
                "classification": classification,
                "handle_time_s": handle_s,
                "time_to_first_quote_s": (
                    max(0.0, (first_quote_ts - session_start).total_seconds())
                    if (first_quote_ts and session_start)
                    else None
                ),
                "completed_at": completed_at,
            }
        )

    payload = {
        "db": _db_summary(),
        "hr": {
            "avg_handle_time_s": (mean(handle_times) if handle_times else 0.0),
            "avg_time_to_first_quote_s": (mean(ttfq_times) if ttfq_times else 0.0),
            "classification_counts": classification_counts,
            "sample_runs": sample_rows,
        },
    }
    return JSONResponse(payload)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_html(token: Optional[str] = Query(None)):
    # Simple gate
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        return HTMLResponse("<h3>Unauthorized</h3>", status_code=401)

    # Safely embed the token as a JS string literal
    js_token = json.dumps(DASHBOARD_TOKEN or "")

    html = """
<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>Inbound Carrier Sales – Dashboard</title>
<style>
body { font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }
.grid { display: grid; grid-template-columns: repeat(2, minmax(260px, 1fr)); gap: 16px; }
.card { border: 1px solid #ddd; border-radius: 12px; padding: 16px; }
h1 { margin: 0 0 16px 0; }
table { border-collapse: collapse; width: 100%; }
th, td { border-bottom: 1px solid #eee; padding: 8px; text-align: left; }
.kpi { font-size: 28px; font-weight: 700; }
.label { color: #666; font-size: 12px; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
</style>
</head>
<body>

<h1>Inbound Carrier Sales – Dashboard</h1>

<div class="grid">
  <div class="card"><div class="label">DB Calls</div><div id="db_calls" class="kpi">–</div></div>
  <div class="card"><div class="label">Win Rate</div><div id="win_rate" class="kpi">–</div></div>
  <div class="card"><div class="label">Avg Rounds</div><div id="avg_rounds" class="kpi">–</div></div>
  <div class="card"><div class="label">Avg Δ to List ($)</div><div id="avg_delta" class="kpi">–</div></div>
</div>

<div class="grid" style="margin-top:20px;">
  <div class="card"><div class="label">Avg Handle Time (s)</div><div id="avg_handle" class="kpi">–</div></div>
  <div class="card"><div class="label">Avg Time to First Quote (s)</div><div id="avg_ttfq" class="kpi">–</div></div>
</div>

<div class="card" style="margin-top:20px;">
  <div class="label">Classification Counts</div>
  <pre id="class_counts" class="mono">–</pre>
</div>

<div class="card" style="margin-top:20px;">
  <div class="label">Recent Runs (sample)</div>
  <table>
    <thead>
      <tr>
        <th>Run ID</th>
        <th>Classification</th>
        <th title="Total call duration in seconds">Call Duration (sec)</th>
        <th title="Elapsed seconds from call start until the agent first quotes or counters a price">Time to First Quote (sec)</th>
        <th>Completed</th>
      </tr>
    </thead>
    <tbody id="runs"></tbody>
  </table>
</div>

<script>
const DASH_TOKEN = __TOKEN__;
(async function(){
  const res = await fetch('/dashboard/hr/metrics?token=' + DASH_TOKEN);
  if (!res.ok) {
    document.body.innerHTML = '<h3>Unauthorized</h3>';
    return;
  }
  const d = await res.json();

  const fmtPct = x => (x*100).toFixed(1) + '%';
  const put = (id, v) => document.getElementById(id).textContent = v;

  put('db_calls', d.db.calls);
  put('win_rate', fmtPct(d.db.win_rate || 0));
  put('avg_rounds', (d.db.avg_rounds || 0).toFixed(2));
  put('avg_delta', (d.db.avg_delta_to_list || 0).toFixed(2));

  put('avg_handle', (d.hr.avg_handle_time_s || 0).toFixed(1));
  put('avg_ttfq', (d.hr.avg_time_to_first_quote_s || 0).toFixed(1));
  document.getElementById('class_counts').textContent = JSON.stringify(d.hr.classification_counts || {}, null, 2);

  const tb = document.getElementById('runs');
  (d.hr.sample_runs || []).forEach(r => {
    const tr = document.createElement('tr');
    const fmtCell = v => (v != null ? Number(v).toFixed(1) : 'N/A'); // <-- show N/A when null
    tr.innerHTML = `
      <td class="mono">${r.run_id || ''}</td>
      <td>${r.classification || ''}</td>
      <td>${fmtCell(r.handle_time_s)}</td>
      <td>${fmtCell(r.time_to_first_quote_s)}</td>
      <td>${r.completed_at || ''}</td>
    `;
    tb.appendChild(tr);
  });
})();
</script>

</body>
</html>
"""
    # inject the token without touching any other braces
    html = html.replace("__TOKEN__", js_token)
    return HTMLResponse(html)

# ------------------ end HR Metrics + Dashboard ------------------