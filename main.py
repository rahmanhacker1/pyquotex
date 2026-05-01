"""
Quotex OTC Candle Data API
Production-ready REST API for fetching 10+ days historical candle data for all OTC pairs
Author: Custom Build
Version: 1.0.0
"""

import asyncio
import json
import logging
import time
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager
import aiohttp

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import aiofiles

# Fixed import with proper error handling
try:
    from pyquotex.stable_api import Quotex
except ImportError:
    try:
        from pyquotex import Quotex
    except ImportError:
        raise ImportError(
            "Could not import Quotex. Please install: pip install git+https://github.com/cleitonleonel/pyquotex.git"
        )

# ============================================
# CONFIGURATION & CONSTANTS
# ============================================

# Supported OTC Pairs - Complete list as requested
SUPPORTED_OTC_PAIRS: List[str] = [
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

# Valid candle periods in seconds
VALID_PERIODS: List[int] = [5, 10, 15, 30, 60, 120, 180, 240, 300, 600, 900, 1800, 3600, 14400, 86400]

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("quotex-api")

# ============================================
# PROXY MANAGER
# ============================================

class ProxyManager:
    """Manages proxy loading and rotation"""
    
    def __init__(self, proxy_file: str = "proxy.txt"):
        self.proxy_file = proxy_file
        self.proxies: List[Dict[str, str]] = []
        self.current_index = 0
        self._load_proxies()
    
    def _load_proxies(self):
        """Load proxies from file"""
        if not os.path.exists(self.proxy_file):
            logger.warning(f"Proxy file {self.proxy_file} not found. Creating empty file.")
            with open(self.proxy_file, 'w') as f:
                f.write("# Add your proxies here\n")
                f.write("# Format: ip:port:username:password\n")
                f.write("# Or: ip:port\n")
            return
        
        try:
            with open(self.proxy_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        proxy_info = self._parse_proxy(line)
                        if proxy_info:
                            self.proxies.append(proxy_info)
            
            logger.info(f"Loaded {len(self.proxies)} proxies from {self.proxy_file}")
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
    
    def _parse_proxy(self, proxy_string: str) -> Optional[Dict[str, str]]:
        """Parse proxy string in format ip:port:username:password or ip:port"""
        parts = proxy_string.split(':')
        
        if len(parts) == 2:
            return {
                'host': parts[0],
                'port': parts[1],
                'auth': None
            }
        elif len(parts) == 4:
            return {
                'host': parts[0],
                'port': parts[1],
                'auth': f"{parts[2]}:{parts[3]}"
            }
        else:
            logger.warning(f"Invalid proxy format: {proxy_string}")
            return None
    
    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        """Get next proxy in rotation"""
        if not self.proxies:
            return None
        
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy
    
    def get_proxy_url(self) -> Optional[str]:
        """Get proxy in URL format"""
        proxy = self.get_next_proxy()
        if not proxy:
            return None
        
        if proxy['auth']:
            return f"http://{proxy['auth']}@{proxy['host']}:{proxy['port']}"
        else:
            return f"http://{proxy['host']}:{proxy['port']}"
    
    def get_proxy_dict(self) -> Optional[Dict[str, str]]:
        """Get proxy in aiohttp format"""
        proxy = self.get_next_proxy()
        if not proxy:
            return None
        
        proxy_url = f"http://{proxy['host']}:{proxy['port']}"
        return {
            'http': proxy_url,
            'https': proxy_url
        }

# Initialize proxy manager
proxy_manager = ProxyManager("proxy.txt")

# ============================================
# PYDANTIC MODELS
# ============================================

class CandleData(BaseModel):
    """Individual candle data model"""
    timestamp: int = Field(..., description="Unix timestamp")
    datetime: str = Field(..., description="ISO format datetime")
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = 0

class CandleResponse(BaseModel):
    """Standard API response model"""
    success: bool
    asset: str
    period_sec: int
    days_requested: int
    total_candles: int
    data: List[CandleData]
    fetch_time_ms: int
    error: Optional[str] = None
    proxy_used: Optional[str] = None

class MultiAssetRequest(BaseModel):
    """Request model for bulk asset data"""
    assets: List[str] = Field(..., description="List of OTC assets")
    days: int = Field(10, ge=1, le=30, description="Days of data (1-30)")
    period: int = Field(60, description="Candle period in seconds")

class AvailablePairsResponse(BaseModel):
    """Response for available pairs endpoint"""
    total: int
    pairs: List[str]
    supported_periods: List[int]

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    supported_assets: int
    connection_test: Dict[str, Any]
    proxies_available: int

# ============================================
# CONNECTION MANAGER
# ============================================

class QuotexConnectionManager:
    """
    Manages Quotex client connections with automatic reconnection
    and proxy support
    """
    
    def __init__(self, email: Optional[str] = None, password: Optional[str] = None):
        self.email = email
        self.password = password
        self._client: Optional[Quotex] = None
        self._lock = asyncio.Lock()
        self._last_connect_time = 0
        self._connection_attempts = 0
        self._max_attempts = 5
        self._current_proxy = None
    
    async def get_client(self, use_proxy: bool = True) -> Quotex:
        """Get or create a connected Quotex client"""
        async with self._lock:
            if self._client is None or time.time() - self._last_connect_time > 300:
                await self._create_and_connect_client(use_proxy)
            return self._client
    
    async def _create_and_connect_client(self, use_proxy: bool = True):
        """Create client and attempt connection with proxy support"""
        for attempt in range(self._max_attempts):
            try:
                # Get proxy if enabled and available
                proxy_url = None
                if use_proxy:
                    proxy_url = proxy_manager.get_proxy_url()
                    if proxy_url:
                        self._current_proxy = proxy_url
                        logger.info(f"Using proxy: {proxy_url}")
                    else:
                        logger.warning("No proxies available, connecting directly")
                
                # Create client with proxy support
                client_kwargs = {
                    "email": self.email,
                    "password": self.password,
                    "lang": "en"
                }
                
                # Try to set proxy if the library supports it
                try:
                    # Check if we can patch the session before creating client
                    import httpx
                    if proxy_url:
                        # Create custom transport with proxy
                        proxy_parts = proxy_url.replace('http://', '').split('@')
                        if len(proxy_parts) == 2:
                            auth, host_port = proxy_parts
                            username, password = auth.split(':')
                            host, port = host_port.split(':')
                            
                            proxy_config = httpx.Proxy(
                                url=f"http://{host}:{port}",
                                auth=(username, password) if username != 'None' else None
                            )
                            
                            # We'll need to monkey patch the client's session
                            self._proxy_config = proxy_config
                        else:
                            self._proxy_config = httpx.Proxy(url=proxy_url)
                except Exception as e:
                    logger.warning(f"Could not configure proxy: {e}")
                    self._proxy_config = None
                
                self._client = Quotex(**client_kwargs)
                
                # Apply proxy if we have it
                if hasattr(self, '_proxy_config') and self._proxy_config:
                    try:
                        # Attempt to patch the client's HTTP session
                        if hasattr(self._client, 'session'):
                            self._client.session.proxies = {
                                'http://': proxy_url,
                                'https://': proxy_url
                            }
                    except Exception as e:
                        logger.warning(f"Could not patch session proxy: {e}")
                
                check, reason = await self._client.connect()
                if check:
                    self._last_connect_time = time.time()
                    self._connection_attempts = 0
                    logger.info(f"Connected to Quotex successfully")
                    return True
                else:
                    logger.warning(f"Connection attempt {attempt + 1} failed: {reason}")
                    
            except Exception as e:
                logger.error(f"Connection error on attempt {attempt + 1}: {str(e)}")
            
            if attempt < self._max_attempts - 1:
                await asyncio.sleep(2 ** attempt)
        
        self._connection_attempts += 1
        raise Exception(f"Failed to connect after {self._max_attempts} attempts")
    
    def get_current_proxy(self) -> Optional[str]:
        """Get currently used proxy"""
        return self._current_proxy
    
    async def close(self):
        """Close the client connection"""
        async with self._lock:
            if self._client:
                try:
                    await self._client.close()
                except Exception as e:
                    logger.error(f"Error closing client: {str(e)}")
                finally:
                    self._client = None
                    self._last_connect_time = 0

# Global connection manager instance
conn_manager = QuotexConnectionManager(
    email="hia32446@gmail.com",
    password="hia32446@gmail.com"
)

# ============================================
# HELPER FUNCTIONS
# ============================================

def timestamp_to_datetime(ts: int) -> str:
    """Convert unix timestamp to ISO format string"""
    return datetime.fromtimestamp(ts).isoformat()

def validate_asset(asset: str) -> bool:
    """Validate if asset is supported"""
    return asset in SUPPORTED_OTC_PAIRS

def validate_period(period: int) -> bool:
    """Validate if candle period is supported"""
    return period in VALID_PERIODS

def process_candle_data(raw_candles: List[Dict], period: int) -> List[CandleData]:
    """
    Process raw candle data from Quotex into standardized format
    Handles both pre-aggregated and tick-level data
    """
    processed = []
    
    for candle in raw_candles:
        # Handle different data formats from Quotex
        if isinstance(candle, dict):
            ts = candle.get('time') or candle.get('timestamp') or candle.get('from')
            if ts is None:
                continue
                
            processed.append(CandleData(
                timestamp=int(ts),
                datetime=timestamp_to_datetime(int(ts)),
                open=float(candle.get('open', 0)),
                high=float(candle.get('high', candle.get('max', 0))),
                low=float(candle.get('low', candle.get('min', 0))),
                close=float(candle.get('close', 0)),
                volume=float(candle.get('volume', 0))
            ))
        elif hasattr(candle, 'time'):
            processed.append(CandleData(
                timestamp=int(candle.time),
                datetime=timestamp_to_datetime(int(candle.time)),
                open=float(getattr(candle, 'open', 0)),
                high=float(getattr(candle, 'high', 0)),
                low=float(getattr(candle, 'low', 0)),
                close=float(getattr(candle, 'close', 0)),
                volume=float(getattr(candle, 'volume', 0))
            ))
    
    return processed

async def fetch_historical_candles(
    asset: str,
    days: int,
    period: int
) -> tuple[List[CandleData], int]:
    """
    Fetch historical candle data for a specific asset
    
    Returns:
        Tuple of (candles_list, fetch_time_ms)
    """
    start_time = time.time()
    client = await conn_manager.get_client()
    
    # Calculate required candles
    candles_per_day = (24 * 60 * 60) // period
    total_candles_needed = days * candles_per_day
    
    all_candles = []
    
    try:
        # Fetch candles using progressive method
        batch_size = min(total_candles_needed, 1000)
        remaining = total_candles_needed
        
        while remaining > 0:
            current_batch = min(batch_size, remaining)
            
            try:
                candles = await client.get_candles(
                    asset=asset,
                    period=period,
                    offset=remaining - current_batch,
                    count=current_batch
                )
                
                if candles:
                    all_candles.extend(candles if isinstance(candles, list) else [candles])
                
                remaining -= current_batch
                
                # Small delay between batches to avoid rate limiting
                if remaining > 0:
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error in batch fetch for {asset}: {str(e)}")
                break
        
        fetch_time = int((time.time() - start_time) * 1000)
        
        # Process and return
        processed = process_candle_data(all_candles, period)
        return processed, fetch_time
        
    except Exception as e:
        logger.error(f"Error fetching candles for {asset}: {str(e)}")
        raise

async def fetch_all_assets_parallel(
    assets: List[str],
    days: int,
    period: int,
    max_concurrent: int = 5
) -> Dict[str, Any]:
    """
    Fetch data for multiple assets in parallel with concurrency control
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_single(asset: str):
        async with semaphore:
            try:
                candles, fetch_time = await fetch_historical_candles(asset, days, period)
                return {
                    "asset": asset,
                    "success": True,
                    "total_candles": len(candles),
                    "data": candles,
                    "fetch_time_ms": fetch_time
                }
            except Exception as e:
                logger.error(f"Failed to fetch {asset}: {str(e)}")
                return {
                    "asset": asset,
                    "success": False,
                    "error": str(e),
                    "total_candles": 0,
                    "data": []
                }
    
    tasks = [fetch_single(asset) for asset in assets]
    results = await asyncio.gather(*tasks)
    
    return {
        "total_assets": len(assets),
        "successful": sum(1 for r in results if r["success"]),
        "failed": sum(1 for r in results if not r["success"]),
        "results": results
    }

# ============================================
# FASTAPI LIFESPAN
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("Starting Quotex OTC Candle API...")
    logger.info(f"Proxies loaded: {len(proxy_manager.proxies)}")
    try:
        yield
    finally:
        logger.info("Shutting down Quotex OTC Candle API...")
        await conn_manager.close()

# ============================================
# FASTAPI APP INITIALIZATION
# ============================================

app = FastAPI(
    title="Quotex OTC Candle Data API",
    description="Production-ready API for fetching 10+ days historical OTC candle data",
    version="1.0.0",
    lifespan=lifespan,
    debug=False
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# API ENDPOINTS
# ============================================

@app.get("/", tags=["Root"])
async def root():
    """API root endpoint with documentation"""
    return {
        "name": "Quotex OTC Candle Data API",
        "version": "1.0.0",
        "endpoints": {
            "/health": "Health check",
            "/pairs": "List all supported OTC pairs",
            "/candles": "Get historical candle data",
            "/candles/stream": "Stream candle data as JSON",
            "/candles/bulk": "Fetch multiple assets at once",
            "/proxy/test": "Test proxy connectivity",
            "/proxy/reload": "Reload proxy list"
        },
        "supported_periods": VALID_PERIODS,
        "proxies_available": len(proxy_manager.proxies)
    }

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check(
    use_proxy: bool = Query(True, description="Enable proxy for connection test")
):
    """Check API health and Quotex connection status"""
    connection_test = {"status": "unknown", "message": ""}
    
    try:
        client = await conn_manager.get_client(use_proxy=use_proxy)
        # Test connection by getting balance
        try:
            balance = await client.get_balance()
            connection_test = {
                "status": "connected",
                "balance_demo": balance,
                "message": "Successfully connected to Quotex",
                "proxy_used": conn_manager.get_current_proxy()
            }
        except AttributeError:
            connection_test = {
                "status": "connected",
                "message": "Successfully connected to Quotex (balance check skipped)",
                "proxy_used": conn_manager.get_current_proxy()
            }
    except Exception as e:
        connection_test = {
            "status": "disconnected",
            "message": str(e),
            "proxy_used": conn_manager.get_current_proxy()
        }
    
    return HealthResponse(
        status="healthy" if connection_test["status"] == "connected" else "degraded",
        timestamp=datetime.now().isoformat(),
        supported_assets=len(SUPPORTED_OTC_PAIRS),
        connection_test=connection_test,
        proxies_available=len(proxy_manager.proxies)
    )

@app.get("/pairs", response_model=AvailablePairsResponse, tags=["Information"])
async def list_available_pairs():
    """Get list of all supported OTC trading pairs"""
    return AvailablePairsResponse(
        total=len(SUPPORTED_OTC_PAIRS),
        pairs=SUPPORTED_OTC_PAIRS,
        supported_periods=VALID_PERIODS
    )

@app.get("/proxy/test", tags=["Proxy"])
async def test_proxy():
    """Test proxy connectivity"""
    if not proxy_manager.proxies:
        return {
            "success": False,
            "message": "No proxies configured",
            "help": "Add proxies to proxy.txt file"
        }
    
    # Test each proxy
    results = []
    for i, proxy in enumerate(proxy_manager.proxies[:5]):  # Test first 5 proxies
        try:
            proxy_url = proxy_manager.get_proxy_url()
            # Simple HTTP test with the proxy
            connector = aiohttp.TCPConnector()
            async with aiohttp.ClientSession(connector=connector) as session:
                start_time = time.time()
                async with session.get('http://httpbin.org/ip', proxy=proxy_url, timeout=10) as resp:
                    response_data = await resp.json()
                    latency = int((time.time() - start_time) * 1000)
                    results.append({
                        "proxy_index": i,
                        "proxy": f"{proxy['host']}:{proxy['port']}",
                        "status": "working",
                        "latency_ms": latency,
                        "ip": response_data.get('origin', 'unknown')
                    })
        except Exception as e:
            results.append({
                "proxy_index": i,
                "proxy": f"{proxy['host']}:{proxy['port']}",
                "status": "failed",
                "error": str(e)
            })
    
    return {
        "success": True,
        "proxies_tested": len(results),
        "proxies_working": sum(1 for r in results if r['status'] == 'working'),
        "proxies_failed": sum(1 for r in results if r['status'] == 'failed'),
        "results": results
    }

@app.get("/proxy/reload", tags=["Proxy"])
async def reload_proxies():
    """Reload proxy list from file"""
    global proxy_manager
    proxy_manager = ProxyManager("proxy.txt")
    return {
        "success": True,
        "message": "Proxies reloaded",
        "total_proxies": len(proxy_manager.proxies)
    }

@app.get("/candles", response_model=CandleResponse, tags=["Candle Data"])
async def get_candles(
    asset: str = Query("EURUSD_otc", description="OTC Asset name"),
    days: int = Query(10, ge=1, le=30, description="Number of days (1-30)"),
    period: int = Query(60, description="Candle period in seconds"),
    use_proxy: bool = Query(True, description="Use proxy for connection")
):
    """
    Fetch historical candle data for a single OTC asset
    
    - **asset**: OTC pair name (e.g., EURUSD_otc, BTCUSD_otc)
    - **days**: Number of days of historical data (max 30)
    - **period**: Candle timeframe in seconds (60=1min, 300=5min, etc.)
    """
    # Validate inputs
    if not validate_asset(asset):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid asset. Use /pairs endpoint to see supported assets."
        )
    
    if not validate_period(period):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Supported periods: {VALID_PERIODS}"
        )
    
    try:
        candles, fetch_time = await fetch_historical_candles(asset, days, period)
        current_proxy = conn_manager.get_current_proxy()
        
        return CandleResponse(
            success=True,
            asset=asset,
            period_sec=period,
            days_requested=days,
            total_candles=len(candles),
            data=candles,
            fetch_time_ms=fetch_time,
            proxy_used="yes" if current_proxy else "no"
        )
    except Exception as e:
        logger.error(f"Error in get_candles for {asset}: {str(e)}")
        return CandleResponse(
            success=False,
            asset=asset,
            period_sec=period,
            days_requested=days,
            total_candles=0,
            data=[],
            fetch_time_ms=0,
            error=str(e),
            proxy_used=conn_manager.get_current_proxy()
        )

@app.get("/candles/stream", tags=["Candle Data"])
async def stream_candles(
    asset: str = Query("EURUSD_otc", description="OTC Asset name"),
    days: int = Query(10, ge=1, le=30),
    period: int = Query(60)
):
    """
    Stream candle data as newline-delimited JSON (NDJSON)
    Useful for large datasets
    """
    if not validate_asset(asset):
        raise HTTPException(status_code=400, detail="Invalid asset")
    
    async def generate_stream():
        try:
            candles, _ = await fetch_historical_candles(asset, days, period)
            
            # Send metadata first
            yield json.dumps({
                "type": "metadata",
                "asset": asset,
                "days": days,
                "period": period,
                "total_candles": len(candles)
            }) + "\n"
            
            # Stream each candle
            for candle in candles:
                yield json.dumps({
                    "type": "candle",
                    "data": candle.model_dump()
                }) + "\n"
                
            # End of stream
            yield json.dumps({"type": "end"}) + "\n"
            
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"
    
    return StreamingResponse(
        generate_stream(),
        media_type="application/x-ndjson",
        headers={
            "Content-Disposition": f"attachment; filename={asset}_{days}d_{period}s.jsonl"
        }
    )

@app.post("/candles/bulk", tags=["Candle Data"])
async def get_bulk_candles(request: MultiAssetRequest):
    """
    Fetch candle data for multiple assets simultaneously
    
    Maximum 10 assets per request to prevent overload
    """
    if len(request.assets) > 10:
        raise HTTPException(
            status_code=400,
            detail="Maximum 10 assets per bulk request"
        )
    
    # Validate all assets
    invalid_assets = [a for a in request.assets if not validate_asset(a)]
    if invalid_assets:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid assets: {invalid_assets}"
        )
    
    if not validate_period(request.period):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Supported: {VALID_PERIODS}"
        )
    
    start_time = time.time()
    results = await fetch_all_assets_parallel(
        request.assets,
        request.days,
        request.period
    )
    total_time = int((time.time() - start_time) * 1000)
    
    return {
        "success": True,
        "total_time_ms": total_time,
        **results
    }

@app.get("/candles/{asset}/latest", tags=["Candle Data"])
async def get_latest_candle(
    asset: str,
    period: int = Query(60, description="Candle period")
):
    """
    Get only the most recent completed candle for an asset
    """
    if not validate_asset(asset):
        raise HTTPException(status_code=400, detail="Invalid asset")
    
    try:
        client = await conn_manager.get_client()
        candles = await client.get_realtime_candles(asset, period)
        
        if candles and len(candles) > 0:
            processed = process_candle_data([candles[-1]], period)
            return {
                "success": True,
                "asset": asset,
                "period_sec": period,
                "candle": processed[0].model_dump() if processed else None
            }
        
        return {"success": False, "message": "No candle data available"}
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/export/{asset}", tags=["Export"])
async def export_candles_json(
    asset: str,
    days: int = Query(10),
    period: int = Query(60)
):
    """
    Export candle data as downloadable JSON file
    """
    if not validate_asset(asset):
        raise HTTPException(status_code=400, detail="Invalid asset")
    
    candles, _ = await fetch_historical_candles(asset, days, period)
    
    export_data = {
        "asset": asset,
        "period_seconds": period,
        "days": days,
        "generated_at": datetime.now().isoformat(),
        "total_candles": len(candles),
        "candles": [c.model_dump() for c in candles]
    }
    
    filename = f"{asset}_{days}d_{period}s_{int(time.time())}.json"
    
    return JSONResponse(
        content=export_data,
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )

# ============================================
# ERROR HANDLERS
# ============================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.detail,
            "status_code": exc.status_code
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Internal server error",
            "detail": str(exc)
        }
    )

# ============================================
# MAIN ENTRY POINT
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=0,
        reload=False,
        log_level="info"
    )
