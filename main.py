"""
Quotex OTC Candle Data API — Cloudflare Bypass, Proxy Support, Real Data
Version: 2.0.5  |  HTTP/2 + sec-* headers to fix 403
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

PROXY_URL = os.getenv("QUOTEX_PROXY")
if PROXY_URL:
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    os.environ["WS_PROXY"] = PROXY_URL
    os.environ["WSS_PROXY"] = PROXY_URL
    os.environ["NO_PROXY"] = "localhost,127.0.0.1"
    print(f"[proxy] Global proxy set: {PROXY_URL}")
else:
    print("[proxy] No QUOTEX_PROXY set. Direct connection will be used.")

import asyncio
import json
import logging
import time

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from pyquotex.api import QuotexAPI as Quotex

# ── Patch Quotex constructor to increase httpx timeouts ──
_original_init = Quotex.__init__
def _patched_init(self, *args, **kwargs):
    _original_init(self, *args, **kwargs)
    for attr in dir(self):
        try:
            obj = getattr(self, attr)
            if isinstance(obj, httpx.AsyncClient):
                obj.timeout = httpx.Timeout(60.0, connect=30.0)
                print(f"[patch] Increased timeout on {attr}")
        except:
            pass
Quotex.__init__ = _patched_init

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("quotex-api")

SUPPORTED_OTC_PAIRS = [
    "AUDCAD_otc", "AUDJPY_otc", "AUDNZD_otc", "AUDUSD_otc", "BRLUSD_otc",
    "CADCHF_otc", "CADJPY_otc", "CHFJPY_otc", "EURAUD_otc", "EURCAD_otc",
    "EURCHF_otc", "EURGBP_otc", "EURJPY_otc", "EURNZD_otc", "EURSGD_otc",
    "EURUSD_otc", "GBPAUD_otc", "GBPCAD_otc", "GBPCHF_otc", "GBPJPY_otc",
    "GBPUSD_otc", "NZDUSD_otc", "USDARS_otc", "USDBDT_otc", "USDCAD_otc",
    "USDCHF_otc", "USDEGP_otc", "USDGBP_otc", "USDIDR_otc", "USDINR_otc",
    "USDJPY_otc", "USDMXN_otc", "USDNGN_otc", "USDPKR_otc", "USDTRY_otc",
    "USDZAR_otc", "USDPHP_otc", "BTCUSD_otc", "BCHUSD_otc", "ARBUSD_otc",
    "ZECUSD_otc", "ATOUSD_otc", "AXSUSD_otc", "XAUUSD_otc", "XAGUSD_otc",
    "USCrude_otc", "UKBrent_otc", "FLOUSD_otc", "AXP_otc", "PFE_otc",
    "INTC_otc", "JNJ_otc", "MCD_otc", "FB_otc", "BA_otc", "MSFT_otc"
]

VALID_PERIODS = [5, 10, 15, 30, 60, 120, 180, 240, 300, 600, 900, 1800, 3600, 14400, 86400]

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://qxbroker.com/en/sign-in",
    "sec-ch-ua": "\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "cache-control": "max-age=0",
}

class CandleData(BaseModel):
    timestamp: int
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

class CandleResponse(BaseModel):
    success: bool
    asset: str
    period_sec: int
    days_requested: int
    total_candles: int
    data: List[CandleData]
    fetch_time_ms: Optional[int] = None
    error: Optional[str] = None

class MultiAssetRequest(BaseModel):
    assets: List[str]
    days: int = Field(10, ge=1, le=30)
    period: int = Field(60)

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    supported_assets: int
    connection: str
    proxy_used: bool

class QuotexConnectionManager:
    def __init__(self):
        self._client: Optional[Quotex] = None
        self._lock = asyncio.Lock()
        self._connected = False
        self._last_connect = 0.0
        self._last_error = ""

    async def get_client(self) -> Quotex:
        async with self._lock:
            now = time.time()
            if self._client is None or (now - self._last_connect > 300):
                email = os.getenv("QUOTEX_EMAIL")
                password = os.getenv("QUOTEX_PASSWORD")
                host = os.getenv("QUOTEX_HOST", "qxbroker.com")
                if not email or not password:
                    raise Exception("Missing QUOTEX_EMAIL/QUOTEX_PASSWORD")

                client = Quotex(host=host, username=email, password=password, lang="en")
                self._inject_headers(client)

                is_demo = os.getenv("QUOTEX_DEMO_MODE", "false").lower() == "true"

                for attempt in range(5):
                    try:
                        logger.info(f"Connection attempt {attempt+1} (demo={is_demo})...")
                        ok, reason = await client.connect(is_demo=is_demo)
                        if ok:
                            self._client = client
                            self._connected = True
                            self._last_connect = time.time()
                            self._last_error = ""
                            logger.info("✅ Connected to Quotex")
                            return client
                        else:
                            self._last_error = f"Quotex returned failure: {reason}"
                            logger.warning(self._last_error)
                    except Exception as e:
                        self._last_error = str(e)
                        logger.error(f"Attempt {attempt+1} error: {e}")
                    await asyncio.sleep(2 ** attempt)

                raise Exception(f"Could not connect after 5 retries. Last error: {self._last_error}")
            return self._client

    def _inject_headers(self, client: Quotex):
        try:
            if hasattr(client, "session"):
                client.session.headers.update(BROWSER_HEADERS)
            if hasattr(client, "http_client") and isinstance(client.http_client, httpx.AsyncClient):
                client.http_client.headers.update(BROWSER_HEADERS)
                # Upgrade to HTTP/2
                transport = httpx.AsyncHTTPTransport(retries=3, http2=True)
                client.http_client._transport = transport
                logger.info("HTTP/2 transport enabled on http_client")
            # Also patch any other httpx client found
            for attr in dir(client):
                try:
                    obj = getattr(client, attr)
                    if isinstance(obj, httpx.AsyncClient):
                        obj.headers.update(BROWSER_HEADERS)
                        if not hasattr(obj._transport, "http2") or not obj._transport.http2:
                            obj._transport = httpx.AsyncHTTPTransport(retries=3, http2=True)
                        logger.info(f"HTTP/2 transport enabled on {attr}")
                except:
                    pass
        except Exception as e:
            logger.warning(f"Inject headers error: {e}")

    async def close(self):
        async with self._lock:
            if self._client:
                try:
                    await self._client.close()
                except:
                    pass
                finally:
                    self._client = None
                    self._connected = False

conn_manager = QuotexConnectionManager()

def raw_to_candle(c: Dict) -> CandleData:
    ts = c.get("time") or c.get("timestamp") or c.get("from")
    if ts is None:
        raise ValueError("Candle missing timestamp")
    ts = int(ts)
    return CandleData(
        timestamp=ts,
        datetime=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        open=float(c.get("open", 0)),
        high=float(c.get("high", c.get("max", 0))),
        low=float(c.get("low", c.get("min", 0))),
        close=float(c.get("close", 0)),
        volume=float(c.get("volume", 0))
    )

async def fetch_historical(asset, days, period):
    client = await conn_manager.get_client()
    total_candles = days * (24 * 60 * 60 // period)
    batch_size = min(total_candles, 1000)
    all_raw = []
    for _ in range(0, total_candles, batch_size):
        batch = await client.get_candles_progressive(
            asset=asset,
            period=period,
            total_candles=min(batch_size, total_candles - len(all_raw))
        )
        if batch:
            all_raw.extend(batch)
        await asyncio.sleep(0.1)
    return [raw_to_candle(r) for r in all_raw]

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API starting...")
    yield
    logger.info("Shutting down...")
    await conn_manager.close()

app = FastAPI(title="Quotex OTC Candle API v2.0.5", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return {"name": "Quotex OTC Candle API", "version": "2.0.5", "docs": "/docs"}

@app.get("/proxy-check")
async def proxy_check():
    proxy = os.getenv("QUOTEX_PROXY")
    if not proxy:
        return {"proxy_configured": False}
    try:
        import requests
        r = requests.get("https://httpbin.org/ip", proxies={"http": proxy, "https": proxy}, timeout=10)
        return {"proxy_configured": True, "public_ip": r.json().get("origin"), "status": r.status_code}
    except Exception as e:
        return {"proxy_configured": True, "error": str(e)}

@app.get("/debug-login")
async def debug_login():
    try:
        client = await conn_manager.get_client()
        candles = await client.get_candles_progressive("EURUSD_otc", period=60, total_candles=5)
        return {"status": "connected", "sample_candles": len(candles)}
    except Exception as e:
        return {"status": "failed", "error": str(e)}

@app.get("/health", response_model=HealthResponse)
async def health():
    conn_status = "disconnected"
    try:
        await conn_manager.get_client()
        conn_status = "connected"
    except Exception as e:
        logger.error(f"Health error: {e}")
    return HealthResponse(
        status="ok" if conn_status == "connected" else "degraded",
        timestamp=datetime.now(timezone.utc).isoformat(),
        supported_assets=len(SUPPORTED_OTC_PAIRS),
        connection=conn_status,
        proxy_used=bool(PROXY_URL)
    )

@app.get("/pairs")
async def list_pairs():
    return {"total": len(SUPPORTED_OTC_PAIRS), "pairs": SUPPORTED_OTC_PAIRS, "periods": VALID_PERIODS}

@app.get("/candles", response_model=CandleResponse)
async def get_candles(asset: str = "EURUSD_otc", days: int = 10, period: int = 60):
    if asset not in SUPPORTED_OTC_PAIRS:
        raise HTTPException(400, "Invalid asset")
    if period not in VALID_PERIODS:
        raise HTTPException(400, "Invalid period")
    try:
        t0 = time.time()
        data = await fetch_historical(asset, days, period)
        elapsed = int((time.time() - t0) * 1000)
        return CandleResponse(success=True, asset=asset, period_sec=period, days_requested=days,
                              total_candles=len(data), data=data, fetch_time_ms=elapsed)
    except Exception as e:
        logger.exception("Fetch failed")
        return CandleResponse(success=False, asset=asset, period_sec=period, days_requested=days,
                              total_candles=0, data=[], error=str(e))

@app.get("/candles/stream")
async def stream_candles(asset: str = "EURUSD_otc", days: int = 10, period: int = 60):
    if asset not in SUPPORTED_OTC_PAIRS:
        raise HTTPException(400, "Invalid asset")
    async def event_stream():
        try:
            data = await fetch_historical(asset, days, period)
            yield json.dumps({"type": "meta", "total": len(data)}) + "\n"
            for c in data:
                yield json.dumps({"type": "candle", **c.model_dump()}) + "\n"
            yield json.dumps({"type": "end"}) + "\n"
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"
    return StreamingResponse(event_stream(), media_type="application/x-ndjson")

@app.post("/candles/bulk")
async def bulk_candles(req: MultiAssetRequest):
    if len(req.assets) > 10:
        raise HTTPException(400, "Max 10 assets per request")
    invalid = [a for a in req.assets if a not in SUPPORTED_OTC_PAIRS]
    if invalid:
        raise HTTPException(400, f"Invalid assets: {invalid}")
    async def fetch_one(asset):
        try:
            data = await fetch_historical(asset, req.days, req.period)
            return {"asset": asset, "success": True, "total": len(data), "data": [c.model_dump() for c in data]}
        except Exception as e:
            return {"asset": asset, "success": False, "error": str(e)}
    tasks = [fetch_one(a) for a in req.assets]
    results = await asyncio.gather(*tasks)
    return {"results": results}

@app.get("/candles/{asset}/latest")
async def latest_candle(asset: str, period: int = 60):
    if asset not in SUPPORTED_OTC_PAIRS:
        raise HTTPException(400, "Invalid asset")
    try:
        client = await conn_manager.get_client()
        candles = await client.get_realtime_candles(asset, period)
        if candles:
            latest = raw_to_candle(candles[-1])
            return {"success": True, "candle": latest}
        return {"success": False, "message": "No recent candles"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/export/{asset}")
async def export_json(asset: str, days: int = 10, period: int = 60):
    if asset not in SUPPORTED_OTC_PAIRS:
        raise HTTPException(400, "Invalid asset")
    data = await fetch_historical(asset, days, period)
    content = {
        "asset": asset,
        "period_sec": period,
        "days": days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candles": [c.model_dump() for c in data]
    }
    filename = f"{asset}_{days}d_{period}s.json"
    return JSONResponse(content=content, headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.websocket("/ws/{asset}")
async def websocket_candles(ws: WebSocket, asset: str, period: int = 60):
    if asset not in SUPPORTED_OTC_PAIRS:
        await ws.close(code=1003, reason="Invalid asset")
        return
    await ws.accept()
    try:
        client = await conn_manager.get_client()
        stream = await client.get_realtime_candles(asset, period)
        async for raw in stream:
            try:
                candle = raw_to_candle(raw)
                await ws.send_json(candle.model_dump())
            except:
                pass
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        try:
            await ws.close()
        except:
            pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=0, reload=True)
