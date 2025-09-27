import os, sqlite3, time
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Header, Query
from pydantic import BaseModel
import httpx
from dotenv import load_dotenv
import logging, traceback

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
        base = max(base, 0.92 * list_rate)
    return round(base, 2)

@app.post("/api/v1/negotiate")
def negotiate(payload: NegInput, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    f = _floor_rate(payload.loadboard_rate, payload.miles, payload.equipment_type)
    if payload.round_index <= 1:
        counter = max(f, round(0.98 * payload.loadboard_rate, 2))
    elif payload.round_index == 2:
        counter = max(f, round(0.95 * payload.loadboard_rate, 2))
    else:
        counter = f
    if payload.carrier_offer >= counter:
        return {"decision": "accept", "counter_offer": payload.carrier_offer, "reason": "meets threshold"}
    if payload.round_index >= 3:
        return {"decision": "reject", "counter_offer": None, "reason": "exceeded rounds"}
    return {"decision": "counter", "counter_offer": counter, "reason": "within policy"}

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