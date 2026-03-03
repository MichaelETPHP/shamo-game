"""
SHAMO Admin API — FastAPI backend  (v3.1 — Fixed for /shamo/ paths)

Port and API base URL come from .env (PORT, API_BASE_URL).
Run: uvicorn api:app --port $PORT --reload   (PORT from .env)
"""
import os, json, logging, asyncio, secrets as _secrets, uuid
from urllib.parse import quote
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Any, Dict, List
from functools import partial
import httpx as _httpx

from dotenv import load_dotenv
# Load .env from project root (same dir as api.py) so it works regardless of cwd
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_env_path)

from fastapi import FastAPI, HTTPException, Request, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel

from supabase import create_client, Client

try:
    import anthropic
except ImportError:
    anthropic = None

# ─── Config ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("shamo.api")

# All from .env — see .env.example.local for descriptions
PORT = int(os.getenv("PORT", "8001"))
API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/") or f"http://localhost:{PORT}"
# local = redirects to /game, /admin. production/vps = redirects to /shamo/game, /shamo/admin
APP_ENV = (os.getenv("APP_ENV") or "local").strip().lower()
IS_VPS = APP_ENV in ("production", "vps", "prod", "1", "true")

SUPABASE_URL  = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_KEY  = (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY") or "").strip()
ADMIN_TOKEN   = (os.getenv("ADMIN_TOKEN") or "").strip()
BOT_TOKEN     = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
ADMIN_USER    = (os.getenv("ADMIN_USERNAME") or "").strip()
ADMIN_PASS    = (os.getenv("ADMIN_PASSWORD") or "").strip()
if not BOT_TOKEN:
    logger.warning("TELEGRAM_BOT_TOKEN not set in .env — /api/player/avatar-url will return 503")

ANTHROPIC_API_KEY = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
# Claude model id (only if using Anthropic). Override in .env if you get 404.
ANTHROPIC_MODEL = (os.getenv("ANTHROPIC_MODEL") or "claude-3-5-sonnet-20240620").strip()


def _shamo_env_script() -> str:
    """Public config from .env injected into HTML (no separate env.js)."""
    def esc(s):
        return (s or "").replace("\\", "\\\\").replace("'", "\\'").replace("\n", " ").replace("</", "<\\/")
    base = esc(API_BASE_URL)
    port = esc(str(PORT))
    supabase_url = esc(SUPABASE_URL)
    supabase_anon = esc(os.getenv("SUPABASE_ANON_KEY", ""))
    return (
        f"<script>\n"
        f"  window.SHAMO_API_BASE_URL = '{base}';\n"
        f"  window.SHAMO_PORT = '{port}';\n"
        f"  window.SHAMO_SUPABASE_URL = '{supabase_url}';\n"
        f"  window.SHAMO_SUPABASE_ANON_KEY = '{supabase_anon}';\n"
        f"</script>"
    )


class ShamoEnvInjectMiddleware(BaseHTTPMiddleware):
    """Inject config from .env into HTML when serving /admin/* and /game/* (replaces env.js)."""
    PLACEHOLDER = "<!-- SHAMO_ENV -->"
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.scope.get("path") or ""
        if "/admin" not in path and "/game" not in path:
            return response
        if not (response.media_type or "").startswith("text/html"):
            return response
        try:
            body_chunks = []
            async for chunk in response.body_iterator:
                body_chunks.append(chunk)
            body = b"".join(body_chunks)
        except Exception:
            return response
        try:
            text = body.decode("utf-8", errors="replace")
        except Exception:
            return response
        if self.PLACEHOLDER not in text:
            return Response(content=body, status_code=response.status_code, media_type=response.media_type)
        injected = text.replace(self.PLACEHOLDER, _shamo_env_script(), 1)
        return Response(
            content=injected.encode("utf-8"),
            status_code=response.status_code,
            media_type=response.media_type,
        )


# ─── Balance helpers (spin_results.amount_etb where w-status = active) ─────────
def _extract_rpc_scalar(raw) -> float:
    """Extract numeric from RPC response (scalar, list, or dict)."""
    if raw is None or raw == "":
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, list) and len(raw) > 0:
        return _extract_rpc_scalar(raw[0])
    if isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, (int, float)):
                return float(v)
            if v is not None and v != "":
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0

async def _get_active_spin_balance(sb: Client, user_id: str) -> float:
    """
    Total ETB from spin_results WHERE user_id (and w-status='active' when available).
    Tries: RPC get_active_spin_total → table with w-status → RPC get_spin_total_simple → table user_id only.
    """
    uid = str(user_id).strip()
    if not uid:
        return 0.0
    # 1) RPC get_active_spin_total (migration 013)
    try:
        res = await run(lambda: sb.rpc("get_active_spin_total", {"p_user_id": uid}).execute())
        return _extract_rpc_scalar(res.data)
    except Exception as e:
        logger.warning("get_active_spin_total: %s", e)
    # 2) RPC get_spin_total_simple — no w-status (migration 014), always works
    try:
        res = await run(lambda: sb.rpc("get_spin_total_simple", {"p_user_id": uid}).execute())
        return _extract_rpc_scalar(res.data)
    except Exception as e:
        logger.warning("get_spin_total_simple: %s", e)
    # 3) Table: user_id only
    try:
        res = await run(lambda: sb.table("spin_results").select("amount_etb").eq("user_id", uid).execute())
        return sum(float(r.get("amount_etb") or 0) for r in (res.data or []))
    except Exception as e:
        logger.warning("spin_results table: %s", e)
    return 0.0

# ─── Singleton Supabase client ────────────────────────────────────────────────
_sb: Client | None = None

def get_sb() -> Client:
    global _sb
    if _sb is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise HTTPException(500, "SUPABASE_URL / SUPABASE_SERVICE_KEY not set in .env")
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized (singleton)")
    return _sb

def _reset_sb():
    global _sb
    _sb = None
    logger.warning("Supabase client reset — stale connection detected, reconnecting...")

# ─── Thread-pool ──────────────────────────────────────────────────────────────
_pool = ThreadPoolExecutor(max_workers=12)

async def run(fn, *args, **kwargs):
    """Run sync fn in thread-pool. Auto-reconnects on stale HTTP/2 / WinError 10035."""
    loop = asyncio.get_event_loop()
    _fn = partial(fn, *args, **kwargs)
    try:
        return await loop.run_in_executor(_pool, _fn)
    except (_httpx.LocalProtocolError, _httpx.RemoteProtocolError, _httpx.ReadError) as exc:
        logger.warning("HTTP connection error (%s), resetting and retrying...", exc)
        _reset_sb()
        return await loop.run_in_executor(_pool, _fn)

async def gather(*fns):
    """Run multiple sync callables concurrently."""
    return await asyncio.gather(*[run(f) for f in fns])

# ─── Broadcast trigger (fire-and-forget) ─────────────────────────────────────
async def _enrich_game_for_broadcast(sb, game: dict) -> dict:
    """Enrich game dict with company info for broadcast message."""
    cid = game.get("company_id")
    if cid:
        try:
            co = await run(lambda: sb.table("companies").select("name").eq("id", cid).single().execute())
            if co.data:
                game = {**game, "companies": co.data, "company_name": co.data.get("name")}
        except Exception:
            pass
    return game


async def trigger_broadcast(game: dict) -> None:
    """
    Fire-and-forget: notify all users of new game via Telegram bot.
    (Currently unused: notification center is for QR generation only.)
    """
    try:
        from bot import get_bot_app, broadcast_new_game
        app = get_bot_app()
        if app and app.bot:
            stats = await broadcast_new_game(game)
            logger.info("Broadcast complete: %s", stats)
        else:
            logger.warning("Bot not available for broadcast")
    except Exception as e:
        logger.error("Broadcast trigger failed: %s", e)


async def trigger_broadcast_qr(qr: dict, game: dict, company: dict | None = None) -> None:
    """
    Fire-and-forget: notify all users when a QR code is generated (notification center).
    Never blocks the API response. Logs success or failure.
    """
    try:
        from bot import get_bot_app, broadcast_new_qr
        app = get_bot_app()
        if app and app.bot:
            stats = await broadcast_new_qr(qr, game, company)
            logger.info("Broadcast QR complete: %s", stats)
        else:
            logger.warning("Bot not available for QR broadcast")
    except Exception as e:
        logger.error("Broadcast QR trigger failed: %s", e)


# ─── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start bot in same process so get_bot_app() works for broadcasts
    _bot_app = None
    _bot_task = None
    try:
        from telegram import Update
        from bot import build_application, set_bot_app
        _bot_app = build_application()
        set_bot_app(_bot_app)
        await _bot_app.initialize()
        await _bot_app.start()
        _bot_task = asyncio.create_task(_bot_app.updater.start_polling(allowed_updates=Update.ALL_TYPES))
        logger.info("Bot started (in-process) for broadcasts")
    except Exception as e:
        logger.warning("Bot startup skipped (run bot.py separately): %s", e)
    yield
    if _bot_task and _bot_app:
        try:
            await _bot_app.updater.stop()
            await _bot_app.stop()
            await _bot_app.shutdown()
        except Exception:
            pass

# ─── Path rewrite: /shamo/api/* -> /api/* (so VPS/local can use same API under /shamo) ───
class ShamoApiPathRewriteMiddleware:
    """Rewrite /shamo/api/xxx to /api/xxx so routes defined as /api/... work at /shamo/api/..."""
    def __init__(self, app):
        self.app = app
    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            path = scope.get("path") or ""
            if path.startswith("/shamo/api"):
                # New scope so routing sees /api/... (strip "/shamo" = 6 chars)
                new_path = path[6:]
                scope = {**scope, "path": new_path}
                if "raw_path" in scope:
                    scope["raw_path"] = new_path.encode("utf-8") if isinstance(new_path, str) else new_path
        await self.app(scope, receive, send)


# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="SHAMO Admin API", version="3.1.0", docs_url="/api/docs", lifespan=lifespan)

# Order: first added = innermost (runs last before routes). Path rewrite must run before route match.
app.add_middleware(ShamoApiPathRewriteMiddleware)
app.add_middleware(ShamoEnvInjectMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Static files: /game and /admin (and /shamo/game, /shamo/admin for proxy)
_admin_dir = os.path.join(os.path.dirname(__file__), "admin")
_game_dir  = os.path.join(os.path.dirname(__file__), "game")
app.mount("/admin", StaticFiles(directory=_admin_dir, html=True), name="admin")
app.mount("/game",  StaticFiles(directory=_game_dir,  html=True), name="game")
app.mount("/shamo/admin", StaticFiles(directory=_admin_dir, html=True), name="admin-shamo")
app.mount("/shamo/game",  StaticFiles(directory=_game_dir,  html=True), name="game-shamo")

@app.get("/", include_in_schema=False)
def root():
    if IS_VPS:
        return RedirectResponse(url="/shamo/game/index.html")
    return RedirectResponse(url="/game/index.html")

@app.get("/admin-panel", include_in_schema=False)
def admin_redirect():
    if IS_VPS:
        return RedirectResponse(url="/shamo/admin/login.html")
    return RedirectResponse(url="/admin/login.html")

# ─── Auth guard ───────────────────────────────────────────────────────────────
def require_admin(request: Request):
    token = request.headers.get("X-Admin-Token") or request.query_params.get("token")
    if token != ADMIN_TOKEN:
        raise HTTPException(401, "Unauthorized")

# ─── Pydantic schemas ─────────────────────────────────────────────────────────
class LoginReq(BaseModel):
    username: str; password: str

class UserUpdate(BaseModel):
    first_name: Optional[str] = None; last_name: Optional[str] = None
    role: Optional[str] = None; is_active: Optional[bool] = None
    is_banned: Optional[bool] = None; ban_reason: Optional[str] = None
    language_code: Optional[str] = None

class BalanceAdjust(BaseModel):
    amount: float; type: str; note: Optional[str] = ""

class GameCreate(BaseModel):
    title: str = "Tonight's SHAMO"; description: Optional[str] = None
    status: str = "draft"; starts_at: str; ends_at: str; game_date: str
    prize_pool_etb: float = 0.0; max_prize_etb: float = 5700.0
    platform_fee_pct: float = 15.0; player_cap_pct: float = 30.0
    company_id: Optional[str] = None; deposit_id: Optional[str] = None

class GameUpdate(BaseModel):
    title: Optional[str] = None; description: Optional[str] = None
    status: Optional[str] = None; starts_at: Optional[str] = None
    ends_at: Optional[str] = None; game_date: Optional[str] = None
    prize_pool_etb: Optional[float] = None; max_prize_etb: Optional[float] = None
    platform_fee_pct: Optional[float] = None; player_cap_pct: Optional[float] = None
    company_id: Optional[str] = None; deposit_id: Optional[str] = None

class QuestionCreate(BaseModel):
    question_text: str; category: Optional[str] = None
    explanation: Optional[str] = None; icon: str = "🇪🇹"
    company_id: Optional[str] = None; is_sponsored: bool = False
    game_id: Optional[str] = None
    options: list

class QuestionUpdate(BaseModel):
    question_text: Optional[str] = None
    category: Optional[str] = None; explanation: Optional[str] = None
    status: Optional[str] = None; icon: Optional[str] = None
    options: Optional[list] = None  # if set, replace all answer_options (items: {letter, text, is_correct})

class RejectReq(BaseModel):
    reason: Optional[str] = ""


class BulkDeleteQuestionsReq(BaseModel):
    ids: list


class BulkAssignGameReq(BaseModel):
    question_ids: list
    game_id: str


class GenerateQuestionsReq(BaseModel):
    """Request for AI-generated questions (Claude)."""
    categories: List[str] = []
    difficulty: str = "Medium"
    count: int = 10
    language: str = "English"


class CompanyCreate(BaseModel):
    name: str; slug: str; category: Optional[str] = None
    description: Optional[str] = None; contact_email: Optional[str] = None
    contact_phone: Optional[str] = None; website: Optional[str] = None
    logo_url: Optional[str] = None
    primary_color: str = "#E8B84B"; owner_id: Optional[str] = None

class CompanyUpdate(BaseModel):
    name: Optional[str] = None; slug: Optional[str] = None; category: Optional[str] = None
    description: Optional[str] = None; contact_email: Optional[str] = None
    contact_phone: Optional[str] = None; website: Optional[str] = None
    logo_url: Optional[str] = None
    status: Optional[str] = None; primary_color: Optional[str] = None

class TopUpReq(BaseModel):
    amount: float; note: Optional[str] = ""

class WithdrawalUpdate(BaseModel):
    notes: Optional[str] = None

class DenyReq(BaseModel):
    reason: Optional[str] = "Admin denied"

class SettingUpdate(BaseModel):
    settings: Dict[str, Any]

class QRCreateReq(BaseModel):
    game_id: str; company_id: Optional[str] = None; label: Optional[str] = None
    base_url: str; max_scans: int = 0; expiry_hours: int = 24

class QRDeleteReq(BaseModel):
    id: str  # qr_codes.id (UUID)

class QRScanReq(BaseModel):
    token: str; user_id: Optional[str] = None
    telegram_id: Optional[int] = None; phone_number: Optional[str] = None

class SessionStartReq(BaseModel):
    game_id: str; user_id: str; qr_token: Optional[str] = None; telegram_id: Optional[int] = None

class AnswerReq(BaseModel):
    session_id: str; user_id: str; game_id: str
    question_id: str; selected_option_id: Optional[str] = None
    question_number: int; time_taken_ms: Optional[int] = None

class SpinReq(BaseModel):
    session_id: str; user_id: str; game_id: str
    question_number: int; segment_label: str; amount_etb: float

class DepositApproveReq(BaseModel):
    notes: Optional[str] = None

class DepositRejectReq(BaseModel):
    reason: Optional[str] = "Rejected by admin"

class WithdrawReq(BaseModel):
    user_id: str; amount_requested: float
    phone_number: str  # Must match registered phone
    full_name: str     # Telebirr registered name
    bank_account: Optional[str] = None

class PlayerLoginReq(BaseModel):
    telegram_id:       int
    first_name:        str
    last_name:         Optional[str] = None
    telegram_username: Optional[str] = None
    language_code:     Optional[str] = "en"
    photo_url:         Optional[str] = None
    phone_number:      Optional[str] = None
    init_data:         Optional[str] = None

class PhoneReq(BaseModel):
    user_id:      str
    phone_number: str


class PhoneByTelegramReq(BaseModel):
    """Bot or Mini App: save phone (and optionally name/photo) when user shares contact."""
    telegram_id:   int
    phone_number:  str
    first_name:    Optional[str] = None
    last_name:     Optional[str] = None
    telegram_username: Optional[str] = None
    photo_url:     Optional[str] = None

# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/health")
async def health():
    try:
        sb = get_sb()
        await run(lambda: sb.table("users").select("id").limit(1).execute())
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "ok", "db": "disconnected", "error": str(e)}

# Add this route temporarily
async def _test_supabase_impl():
    try:
        sb = get_sb()
        result = await run(lambda: sb.table("users").select("id").limit(1).execute())
        return {"status": "ok", "row_count": len(result.data or []), "error": None}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/test-supabase")
async def test_supabase(): return await _test_supabase_impl()

@app.get("/api/test-supabase")
async def test_supabase_api(): return await _test_supabase_impl()

# ═══════════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/auth/login")
def login(body: LoginReq):
    if body.username == ADMIN_USER and body.password == ADMIN_PASS:
        return {"token": ADMIN_TOKEN, "username": ADMIN_USER, "role": "admin"}
    raise HTTPException(401, "Invalid credentials")

@app.post("/api/auth/logout")
def logout(_=Depends(require_admin)):
    return {"message": "Logged out"}

@app.get("/api/auth/me")
def me(_=Depends(require_admin)):
    return {"username": ADMIN_USER, "role": "admin"}

# ═══════════════════════════════════════════════════════════════════════════════
# PLAYER — public endpoints (no admin token)
# ═══════════════════════════════════════════════════════════════════════════════
def _mask_phone(phone: Optional[str]) -> str:
    """Mask phone for logs: ***1234 (last 4 digits only)."""
    if not phone or not isinstance(phone, str):
        return "none"
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) < 4:
        return "***"
    return "***" + digits[-4:]

@app.post("/api/player/login")
async def player_login(body: PlayerLoginReq):
    """
    Register or update a Telegram user. Requires telegram_id, first_name.
    New users must have phone_number (share phone first via bot or Mini App contact).
    Saves photo_url when provided (profile sync).
    """
    sb = get_sb()
    existing = await run(lambda: sb.table("users")
        .select("*").eq("telegram_id", body.telegram_id).limit(1).execute())

    normalized_phone: Optional[str] = None
    if not existing.data:
        if not body.phone_number or not str(body.phone_number).strip():
            logger.info("[SHAMO] REGISTER_REJECTED telegram_id=%s first_name=%s reason=phone_required", body.telegram_id, body.first_name or "?")
            raise HTTPException(400, detail="Phone number required to register. Please share your phone first.")
        raw = str(body.phone_number).strip()
        digits = "".join(c for c in raw if c.isdigit())
        if not digits or len(digits) < 9:
            raise HTTPException(400, detail="Invalid phone number")
        if digits.startswith("0"):
            digits = "251" + digits[1:]
        elif not digits.startswith("251") and len(digits) == 9:
            digits = "251" + digits
        normalized_phone = "+" + digits

    now = datetime.now(timezone.utc).isoformat()
    upsert_data = {
        "telegram_id":       body.telegram_id,
        "first_name":        body.first_name,
        "last_name":         body.last_name,
        "telegram_username": body.telegram_username,
        "language_code":     body.language_code or "en",
        "is_active":         True,
        "updated_at":        now,
    }
    if normalized_phone:
        upsert_data["phone_number"] = normalized_phone
    elif body.phone_number:
        upsert_data["phone_number"] = body.phone_number
    if body.photo_url and isinstance(body.photo_url, str) and body.photo_url.strip():
        upsert_data["photo_url"] = body.photo_url.strip()

    if existing.data:
        uid = existing.data[0]["id"]
        await run(lambda: sb.table("users").update(upsert_data).eq("id", uid).execute())
        res = await run(lambda: sb.table("users").select("*").eq("id", uid).single().execute())
        user = res.data
        phone_ok = bool(user.get("phone_number"))
        if not phone_ok:
            logger.warning("[SHAMO] LOGIN telegram_id=%s user_id=%s first_name=%s phone=missing photo=%s — share phone for withdrawals", body.telegram_id, uid, body.first_name or "?", "yes" if user.get("photo_url") else "no")
        else:
            logger.info("[SHAMO] LOGIN telegram_id=%s user_id=%s first_name=%s phone=%s photo=%s", body.telegram_id, uid, body.first_name or "?", _mask_phone(user.get("phone_number")), "yes" if user.get("photo_url") else "no")
        return user
    else:
        upsert_data["role"]       = "player"
        upsert_data["created_at"] = now
        upsert_data["photo_url"]  = (body.photo_url or "").strip() or None
        res = await run(lambda: sb.table("users").insert(upsert_data).execute())
        new_user = (res.data or [{}])[0]
        uid = new_user.get("id", "")
        logger.info("[SHAMO] REGISTER telegram_id=%s user_id=%s first_name=%s phone=%s photo=%s", body.telegram_id, uid, body.first_name or "?", _mask_phone(new_user.get("phone_number")), "yes" if new_user.get("photo_url") else "no")
        return new_user

@app.post("/api/player/phone")
async def save_player_phone(body: PhoneReq):
    """Save phone number after Telegram contact share (by user_id)."""
    sb = get_sb()
    await run(lambda: sb.table("users")
        .update({"phone_number": body.phone_number, "updated_at": datetime.now(timezone.utc).isoformat()})
        .eq("id", body.user_id).execute())
    return {"message": "Phone saved", "phone_number": body.phone_number}


@app.post("/api/player/phone-by-telegram")
async def save_phone_by_telegram(body: PhoneByTelegramReq):
    """
    Bot OR Mini App calls this when phone is received.
    Updates user by telegram_id; creates user if not exists.
    Always returns {message, user} with full user object.
    """
    sb = get_sb()
    raw = (body.phone_number or "").strip()
    if not raw:
        raise HTTPException(400, "phone_number required")
    # Normalize: _251 91618 2957, 251916182957, +251... → +251916182957
    digits = "".join(c for c in raw if c.isdigit())
    if not digits or len(digits) < 9:
        raise HTTPException(400, "Invalid phone number")
    if digits.startswith("0"):
        digits = "251" + digits[1:]
    elif not digits.startswith("251") and len(digits) == 9:
        digits = "251" + digits
    phone = "+" + digits

    now = datetime.now(timezone.utc).isoformat()
    existing = await run(lambda: sb.table("users")
        .select("id").eq("telegram_id", body.telegram_id).limit(1).execute())

    if existing.data:
        uid = existing.data[0]["id"]
        update_payload = {"phone_number": phone, "updated_at": now}
        if body.first_name:
            update_payload["first_name"] = body.first_name
        if body.last_name is not None:
            update_payload["last_name"] = body.last_name
        if body.telegram_username is not None:
            update_payload["telegram_username"] = body.telegram_username
        if body.photo_url and isinstance(body.photo_url, str) and body.photo_url.strip():
            update_payload["photo_url"] = body.photo_url.strip()
        await run(lambda: sb.table("users")
            .update(update_payload).eq("id", uid).execute())
        res = await run(lambda: sb.table("users")
            .select("*").eq("id", uid).limit(1).execute())
        user_data = res.data[0] if res.data else {}
        logger.info("[SHAMO] PHONE_SAVED telegram_id=%s user_id=%s first_name=%s phone=%s (updated)", body.telegram_id, uid, user_data.get("first_name") or "?", _mask_phone(phone))
        return {"message": "Phone saved", "user": user_data}
    else:
        row = {
            "telegram_id": body.telegram_id,
            "first_name": (body.first_name or "").strip() or "Player",
            "last_name": (body.last_name or "").strip() or None,
            "telegram_username": (body.telegram_username or "").strip() or None,
            "phone_number": phone,
            "role": "player",
            "created_at": now,
            "updated_at": now,
        }
        if body.photo_url and isinstance(body.photo_url, str) and body.photo_url.strip():
            row["photo_url"] = body.photo_url.strip()
        res = await run(lambda: sb.table("users").insert(row).execute())
        new_user = (res.data or [{}])[0]
        uid = new_user.get("id", "")
        logger.info("[SHAMO] REGISTER telegram_id=%s user_id=%s first_name=%s phone=%s photo=%s (via phone share)", body.telegram_id, uid, row["first_name"], _mask_phone(phone), "yes" if row.get("photo_url") else "no")
        return {"message": "User created with phone", "user": new_user}


@app.get("/api/player/me")
async def player_me(telegram_id: int):
    """Return the current player by Telegram ID (for Mini App to refetch after contact share)."""
    sb = get_sb()
    res = await run(lambda: sb.table("users").select("*").eq("telegram_id", telegram_id).limit(1).execute())
    if not res.data:
        raise HTTPException(404, "User not found")
    return res.data[0]


@app.get("/api/player/avatar-url")
async def player_avatar_url(telegram_id: int):
    """
    Fetch profile photo URL from Telegram Bot API.
    Use when initDataUnsafe.user.photo_url is missing (only available when app launched via attachment menu).
    """
    if not BOT_TOKEN:
        raise HTTPException(503, "Bot token not configured")
    try:
        async with _httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUserProfilePhotos",
                params={"user_id": telegram_id, "limit": 1},
            )
            data = r.json()
            if not data.get("ok") or not data.get("result", {}).get("photos"):
                return {"photo_url": None}
            photos = data["result"]["photos"]
            largest = photos[0][-1]
            file_id = largest["file_id"]
            fr = await client.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                params={"file_id": file_id},
            )
            fdata = fr.json()
            if not fdata.get("ok"):
                return {"photo_url": None}
            path = fdata["result"].get("file_path")
            if not path:
                return {"photo_url": None}
            url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}"
            return {"photo_url": url}
    except Exception as e:
        logger.warning("avatar-url fetch failed for tg_id=%s: %s", telegram_id, e)
        raise HTTPException(500, "Could not fetch avatar")


# Default avatar when user has no Telegram profile photo (gamer-style placeholder)
DEFAULT_AVATAR_URL = "https://static.vecteezy.com/system/resources/thumbnails/054/078/735/small_2x/gamer-avatar-with-headphones-and-controller-vector.jpg"

async def _stream_default_avatar():
    """Fetch and stream the default avatar image (used when user has no Telegram photo)."""
    async with _httpx.AsyncClient(timeout=15) as client:
        r = await client.get(DEFAULT_AVATAR_URL)
        r.raise_for_status()
        content = r.content
        # Vecteezy URL ends in .jpg
        media_type = r.headers.get("content-type") or "image/jpeg"
        if ";" in media_type:
            media_type = media_type.split(";")[0].strip()
        return StreamingResponse(iter([content]), media_type=media_type)

@app.get("/api/player/avatar-image")
async def player_avatar_image(telegram_id: int):
    """
    Proxy: fetch profile photo from Telegram Bot API and stream image bytes.
    If user has no profile photo, returns default gamer avatar image (200 OK).
    """
    if not BOT_TOKEN:
        raise HTTPException(503, "Bot token not configured")
    try:
        async with _httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUserProfilePhotos",
                params={"user_id": telegram_id, "limit": 1},
            )
            data = r.json()
            photos = (data.get("result") or {}).get("photos") if data.get("ok") else []
            if not photos:
                logger.info("[SHAMO] No profile photo for telegram_id=%s — using default avatar", telegram_id)
                return await _stream_default_avatar()
            largest = photos[0][-1]
            file_id = largest["file_id"]
            fr = await client.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                params={"file_id": file_id},
            )
            fdata = fr.json()
            if not fdata.get("ok"):
                logger.info("[SHAMO] getFile failed for telegram_id=%s — using default avatar", telegram_id)
                return await _stream_default_avatar()
            path = (fdata.get("result") or {}).get("file_path")
            if not path:
                return await _stream_default_avatar()
            url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}"
            img_resp = await client.get(url)
            img_resp.raise_for_status()
            content = img_resp.content
            media_type = "image/jpeg"
            if path.lower().endswith(".png"):
                media_type = "image/png"
            return StreamingResponse(iter([content]), media_type=media_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("avatar-image fetch failed for tg_id=%s: %s", telegram_id, e)
        raise HTTPException(500, "Could not fetch avatar image")


class DeleteAccountReq(BaseModel):
    telegram_id: int


@app.post("/api/player/delete-account")
async def player_delete_account(body: DeleteAccountReq):
    """
    Self-service: delete the current player's account and all related data (by telegram_id).
    Order matches schema FKs: game_sessions (cascade round_answers, spin_results), transactions,
    qr_scans, withdrawals, leaderboard, notifications; reassign questions created_by to platform admin;
    then delete user.
    """
    sb = get_sb()
    res = await run(lambda: sb.table("users").select("id").eq("telegram_id", body.telegram_id).limit(1).execute())
    if not res.data:
        raise HTTPException(404, "User not found")
    uid = res.data[0]["id"]
    try:
        # 1) game_sessions — CASCADE deletes round_answers and spin_results
        await run(lambda: sb.table("game_sessions").delete().eq("user_id", uid).execute())
        # 2) transactions — references user_id, no CASCADE
        await run(lambda: sb.table("transactions").delete().eq("user_id", uid).execute())
        # 3) qr_scans — by user_id and by telegram_id (no CASCADE from users)
        await run(lambda: sb.table("qr_scans").delete().eq("user_id", uid).execute())
        await run(lambda: sb.table("qr_scans").delete().eq("telegram_id", body.telegram_id).execute())
        # 4) withdrawals, leaderboard, notifications (explicit; also have ON DELETE CASCADE from users)
        await run(lambda: sb.table("withdrawals").delete().eq("user_id", uid).execute())
        await run(lambda: sb.table("leaderboard").delete().eq("user_id", uid).execute())
        await run(lambda: sb.table("notifications").delete().eq("user_id", uid).execute())
        # 5) questions created by this user — created_by is NOT NULL FK; reassign to platform admin
        admin = await run(lambda: sb.table("users").select("id").eq("telegram_id", 0).limit(1).execute())
        if admin.data:
            await run(lambda: sb.table("questions").update({"created_by": admin.data[0]["id"]}).eq("created_by", uid).execute())
        # 6) companies owned by this user (owner_id ON DELETE CASCADE would do it, but explicit is clear)
        await run(lambda: sb.table("companies").delete().eq("owner_id", uid).execute())
        # 7) audit_log.actor_id — nullable; clear so delete user doesn't fail
        try:
            await run(lambda: sb.table("audit_log").update({"actor_id": None}).eq("actor_id", uid).execute())
        except Exception:
            pass
    except Exception as e:
        logger.warning("player_delete_account pre-user %s: %s", uid, e)
        raise HTTPException(500, str(e))
    try:
        await run(lambda: sb.table("users").delete().eq("id", uid).execute())
        logger.info("player_delete_account: telegram_id=%s uid=%s", body.telegram_id, uid)
        return {"ok": True, "message": "Account deleted"}
    except Exception as e:
        logger.warning("player_delete_account %s: %s", uid, e)
        raise HTTPException(500, str(e))


def _is_valid_uuid(s: str) -> bool:
    """Check if string is a valid UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)."""
    if not s or len(s) != 36: return False
    parts = s.split("-")
    if len(parts) != 5: return False
    for i, n in enumerate([8, 4, 4, 4, 12]):
        if len(parts[i]) != n: return False
        try: int(parts[i], 16)
        except ValueError: return False
    return True


@app.get("/api/player/{uid}/withdrawals/completed")
async def player_withdrawals_completed(uid: str):
    """Completed withdrawals only from withdrawals table (admin released). For history section."""
    if not _is_valid_uuid(uid):
        raise HTTPException(400, f"Invalid user ID format. Use a valid UUID, e.g. from: SELECT id FROM users LIMIT 1")
    try:
        sb = get_sb()
        res = await run(lambda: sb.table("withdrawals").select("*")
            .eq("user_id", uid).eq("status", "completed").order("requested_at", desc=True).limit(20).execute())
        return res.data or []
    except Exception as e:
        logger.exception("player_withdrawals_completed %s: %s", uid, e)
        raise HTTPException(500, str(e))


@app.get("/api/player/{uid}/withdrawals")
async def player_withdrawals(uid: str):
    """All withdrawals from withdrawals table for this user (for pending check)."""
    if not _is_valid_uuid(uid):
        raise HTTPException(400, f"Invalid user ID format. Use a valid UUID, e.g. from: SELECT id FROM users LIMIT 1")
    try:
        sb = get_sb()
        res = await run(lambda: sb.table("withdrawals").select("*")
            .eq("user_id", uid).order("requested_at", desc=True).limit(20).execute())
        return res.data or []
    except Exception as e:
        logger.exception("player_withdrawals %s: %s", uid, e)
        raise HTTPException(500, str(e))

@app.get("/api/player/{uid}/balance")
async def player_balance(uid: str):
    """Balance for withdraw check: SUM(amount_etb) from spin_results WHERE user_id AND w-status='active'."""
    sb = get_sb()
    bal = await _get_active_spin_balance(sb, uid)
    return {"available_balance": round(bal, 2)}

@app.get("/api/player/{uid}/spins")
async def player_spin_history(uid: str):
    sb = get_sb()
    res = await run(lambda: sb.table("spin_results").select(
        "*, games!spin_results_game_id_fkey(title,game_date)"
    ).eq("user_id", uid).order("spun_at", desc=True).limit(30).execute())
    return res.data or []


@app.get("/api/player/{uid}/game-sessions")
async def player_game_sessions(uid: str):
    """Game sessions (for Game History: missed = completed with total_earned=0)."""
    sb = get_sb()
    res = await run(lambda: sb.table("game_sessions").select(
        "id,game_id,total_earned,ended_at,started_at,games(title,game_date)"
    ).eq("user_id", uid).eq("is_completed", True).eq("total_earned", 0).order("ended_at", desc=True).limit(30).execute())
    return res.data or []


@app.get("/api/player/{uid}/balance-summary")
async def player_balance_summary(uid: str, telegram_id: Optional[int] = None):
    """
    Available balance = SUM(amount_etb) from spin_results WHERE user_id AND w-status='active'.
    No withdrawal subtraction — display raw total from active spins.
    """
    sb = get_sb()
    user_res = await run(lambda: sb.table("users").select(
        "id,telegram_id,phone_number,first_name,last_name,games_played,games_won,current_streak"
    ).eq("id", uid).limit(1).execute())
    if not user_res.data:
        raise HTTPException(404, "User not found")
    user = user_res.data[0]

    # 1) Available balance = SUM(amount_etb) from spin_results WHERE w-status='active' (no withdrawal subtraction)
    total_earned = await _get_active_spin_balance(sb, uid)
    available_balance = total_earned

    # 2) Total withdrawn (completed only, for display)
    wds_done = await run(lambda: sb.table("withdrawals").select("amount_paid").eq("user_id", uid).eq("status", "completed").execute())
    total_withdrawn = sum(float(r.get("amount_paid") or 0) for r in (wds_done.data or []))

    # 3) Spin rows for breakdown (w-status='active' only)
    try:
        q = sb.table("spin_results").select(
            "amount_etb,segment_label,question_number,game_id,spun_at,games!spin_results_game_id_fkey(title)"
        ).eq("user_id", uid).eq("w-status", "active").order("spun_at")
        spins_res = await run(lambda: q.execute())
        active_rows = spins_res.data or []
    except Exception:
        try:
            spins_res = await run(lambda: sb.table("spin_results").select(
                "amount_etb,segment_label,question_number,game_id,spun_at,games!spin_results_game_id_fkey(title)"
            ).eq("user_id", uid).order("spun_at").execute())
            active_rows = spins_res.data or []
        except Exception:
            active_rows = []
    # Build active_spins array: segment_amount_ETB per spin
    active_spins = []
    for r in active_rows:
        g = r.get("games") or {}
        active_spins.append({
            "segment_label": r.get("segment_label") or "",
            "amount_etb": round(float(r.get("amount_etb") or 0), 2),
            "game_id": r.get("game_id"),
            "game_title": g.get("title") or "Game",
            "question_number": r.get("question_number"),
        })
    return {
        "user": {
            "id": user["id"],
            "phone_number": user.get("phone_number"),
            "telegram_id": user.get("telegram_id"),
            "games_played": int(user.get("games_played") or 0),
            "games_won": int(user.get("games_won") or 0),
            "current_streak": int(user.get("current_streak") or 0),
        },
        "total_earned_from_spins": round(total_earned, 2),
        "total_withdrawn": round(total_withdrawn, 2),
        "available_balance": round(available_balance, 2),
        "active_spins": active_spins,
        "games_played": int(user.get("games_played") or 0),
        "games_won": int(user.get("games_won") or 0),
        "current_streak": int(user.get("current_streak") or 0),
    }

# ─── Game config from platform_config (for mini-app timer & rules + maintenance) ─
async def _get_game_config(sb: Client) -> dict:
    """Read game settings from platform_config. Includes maintenance mode for mini-app."""
    try:
        keys = [
            "seconds_per_question", "max_wrong_answers", "questions_per_game",
            "maintenance_mode", "maintenance_message", "maintenance_linkedin_url", "maintenance_instagram_url",
        ]
        rows = await run(lambda: sb.table("platform_config").select("key,value").in_("key", keys).execute())
        cfg = {}
        for r in (rows.data or []):
            v = r.get("value")
            if v is None:
                continue
            k = r["key"]
            if isinstance(v, str):
                v_strip = v.strip()
                if v_strip.lower() in ("true", "false"):
                    cfg[k] = v_strip.lower() == "true"
                elif v_strip.startswith(("{", "[")):
                    try:
                        cfg[k] = json.loads(v)
                    except (ValueError, TypeError):
                        cfg[k] = v
                elif k in ("seconds_per_question", "max_wrong_answers", "questions_per_game"):
                    try:
                        cfg[k] = int(float(v))
                    except (ValueError, TypeError):
                        pass
                else:
                    cfg[k] = v
            else:
                cfg[k] = v
        out = {
            "seconds_per_question": max(3, min(30, cfg.get("seconds_per_question") or 4)),
            "max_wrong_answers": max(1, min(10, cfg.get("max_wrong_answers") or 3)),
            "questions_per_game": max(1, min(50, cfg.get("questions_per_game") or 10)),
            "maintenance_mode": bool(cfg.get("maintenance_mode")),
            "maintenance_message": (cfg.get("maintenance_message") or "Michael is updating some features. Just give us some time!").strip(),
            "maintenance_linkedin_url": (cfg.get("maintenance_linkedin_url") or "").strip() or "#",
            "maintenance_instagram_url": (cfg.get("maintenance_instagram_url") or "").strip() or "#",
        }
        return out
    except Exception as e:
        logger.warning("_get_game_config: %s", e)
        return {
            "seconds_per_question": 4, "max_wrong_answers": 3, "questions_per_game": 10,
            "maintenance_mode": False,
            "maintenance_message": "Michael is updating some features. Just give us some time!",
            "maintenance_linkedin_url": "#",
            "maintenance_instagram_url": "#",
        }

@app.get("/api/public/game-config")
async def public_game_config():
    """Public: seconds per question and max wrong answers (for mini-app rules & timer). No auth."""
    sb = get_sb()
    return await _get_game_config(sb)

# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC GAME ENDPOINTS — mini-app reads these (no admin token)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/public/game/active")
async def public_active_game():
    """
    Returns the current active game with company info and top 3 winners.
    Called by mini-app home screen. No auth required.
    """
    sb = get_sb()
    game_res = await run(lambda: sb.table("games").select(
        "id,title,status,game_date,prize_pool_etb,prize_pool_remaining,"
        "platform_fee_pct,player_cap_pct,total_players,total_winners,total_paid_out,"
        "starts_at,ends_at,company_id,"
        "companies!games_company_id_fkey(id,name,logo_url,primary_color)"
    ).eq("status", "active").order("starts_at", desc=True).limit(1).execute())

    games = game_res.data or []
    if not games:
        return {"game": None, "winners": [], "live_claims": []}

    game = games[0]
    game_id = game["id"]

    # Top 3 winners for this game
    winners_res = await run(lambda: sb.table("leaderboard").select(
        "rank,total_earned,questions_correct,"
        "users!leaderboard_user_id_fkey(first_name,last_name,telegram_username)"
    ).eq("game_id", game_id).order("rank").limit(3).execute())

    # Last 10 spin results as live feed (mask names for privacy)
    claims_res = await run(lambda: sb.table("spin_results").select(
        "amount_etb,segment_label,question_number,spun_at,"
        "users!spin_results_user_id_fkey(first_name,last_name)"
    ).eq("game_id", game_id).order("spun_at", desc=True).limit(10).execute())

    winners = []
    for w in (winners_res.data or []):
        u = w.pop("users", None) or {}
        name = f"{u.get('first_name','')}"
        last = u.get("last_name","")
        if last: name += f" {last[0]}."
        winners.append({**w, "display_name": name.strip() or "—"})

    claims = []
    for c in (claims_res.data or []):
        u = c.pop("users", None) or {}
        name = f"{u.get('first_name','')}"
        last = u.get("last_name","")
        if last: name += f" {last[0]}."
        claims.append({**c, "display_name": name.strip() or "—"})

    return {"game": game, "winners": winners, "live_claims": claims}


@app.get("/api/public/games/debug")
async def debug_active_games():
    f"""
    Public debug: shows all games with status, question count, QR count.
    Open http://localhost:{PORT}/api/public/games/debug to diagnose display issues.
    No auth required.
    """
    sb = get_sb()
    games_res = await run(lambda: sb.table("games").select(
        "id,title,status,starts_at,ends_at,updated_at"
    ).order("updated_at", desc=True).limit(20).execute())
    rows = games_res.data or []
    out = []
    for g in rows:
        gq = await run(lambda gi=g["id"]: sb.table("game_questions").select("id", count="exact").eq("game_id", gi).execute())
        qr = await run(lambda gi=g["id"]: sb.table("qr_codes").select("id", count="exact").eq("game_id", gi).eq("status", "active").execute())
        out.append({
            "id":         g["id"],
            "title":      g["title"],
            "status":     g["status"],
            "starts_at":  g.get("starts_at"),
            "ends_at":    g.get("ends_at"),
            "questions":  gq.count or 0,
            "active_qrs": qr.count or 0,
            "will_show":  g["status"] == "active",
        })
    return {"games": out, "tip": "A game shows in the mini-app when status='active'. Click ▶ Go Live in the Admin → Games page to activate."}


@app.get("/api/public/games/active")
async def public_active_games_list():
    """
    Returns all active games with their active QR codes.
    Game status='active' already means admin verified questions+QR before going live.
    No auth required — uses service key (bypasses RLS).
    """
    sb = get_sb()
    try:
        games_res = await run(lambda: sb.table("games").select(
            "id,title,status,game_date,prize_pool_etb,prize_pool_remaining,"
            "total_players,total_winners,total_paid_out,starts_at,ends_at,company_id"
        ).eq("status", "active").order("updated_at", desc=True).limit(10).execute())
        games = games_res.data or []
        logger.info("public_active_games_list: found %d active games", len(games))
    except Exception as e:
        logger.error("public_active_games_list games query FAILED: %s", e)
        return {"games": []}

    # Enrich with company info separately (avoids FK hint issues)
    if games:
        company_ids = list({g["company_id"] for g in games if g.get("company_id")})
        try:
            co_res = await run(lambda: sb.table("companies").select("id,name,logo_url,primary_color")
                .in_("id", company_ids).execute()) if company_ids else type("R", (), {"data": []})()
            co_map = {c["id"]: c for c in (co_res.data or [])}
            for g in games:
                g["companies"] = co_map.get(g.get("company_id")) or {}
        except Exception as e:
            logger.warning("public_active_games_list company query: %s", e)
            for g in games:
                g["companies"] = {}

    if not games:
        return {"games": []}

    game_ids = [g["id"] for g in games]

    try:
        qr_res = await run(lambda: sb.table("qr_codes")
            .select("game_id,token,label,status,qr_url")
            .in_("game_id", game_ids).eq("status", "active")
            .order("created_at").execute())
        qr_rows = qr_res.data or []
    except Exception as e:
        logger.warning("public_active_games_list qr query: %s", e)
        qr_rows = []

    qr_by_game: dict = {}
    for row in qr_rows:
        gid = row["game_id"]
        qr_by_game.setdefault(gid, []).append({
            "token": row["token"], "label": row.get("label"), "qr_url": row.get("qr_url")
        })

    for g in games:
        g["join_codes"] = qr_by_game.get(g["id"], [])[:5]
        first_qr = (qr_by_game.get(g["id"]) or [None])[0]
        g["join_code"] = first_qr["token"] if first_qr else None
        g["has_qr"] = bool(qr_by_game.get(g["id"]))
        co = g.get("companies") or {}
        logger.info("Game '%s' company_id=%s company=%s logo=%s color=%s",
                     g.get("title"), g.get("company_id"), co.get("name"), co.get("logo_url"), co.get("primary_color"))

    return {"games": games}


@app.get("/api/public/games/{game_id}")
async def public_game_by_id(game_id: str):
    """
    Public — single game by id with company (for mini-app join sheet after QR validate).
    No auth. Uses service key. Rejects non-UUID path segments (e.g. "active") so they hit the list route.
    """
    try:
        uuid.UUID(game_id)
    except (ValueError, TypeError):
        raise HTTPException(404, "Game not found")
    sb = get_sb()
    res = await run(lambda: sb.table("games")
        .select("id,title,status,game_date,prize_pool_etb,prize_pool_remaining,total_players,company_id")
        .eq("id", game_id).limit(1).execute())
    rows = res.data or []
    if not rows:
        raise HTTPException(404, "Game not found")
    g = rows[0]
    cid = g.get("company_id")
    if cid:
        co_res = await run(lambda: sb.table("companies")
            .select("id,name,logo_url,primary_color").eq("id", cid).limit(1).execute())
        g["companies"] = (co_res.data or [{}])[0] if co_res.data else {}
    else:
        g["companies"] = {}
    return g


@app.get("/api/public/leaderboard")
async def public_leaderboard(game_id: str = "", limit: int = 20):
    """All-time or per-game leaderboard. No auth required."""
    sb = get_sb()
    if game_id:
        res = await run(lambda: sb.table("leaderboard").select(
            "rank,total_earned,questions_correct,"
            "users!leaderboard_user_id_fkey(first_name,last_name,telegram_username,games_played,games_won)"
        ).eq("game_id", game_id).order("rank").limit(limit).execute())
    else:
        res = await run(lambda: sb.table("users").select(
            "first_name,last_name,telegram_username,total_earned,games_played,games_won,best_streak"
        ).eq("is_active", True).eq("is_banned", False)
        .order("total_earned", desc=True).limit(limit).execute())

    rows = res.data or []
    result = []
    for i, r in enumerate(rows):
        if game_id:
            u = r.pop("users", None) or {}
            name = f"{u.get('first_name','')} {(u.get('last_name') or [''])[0]+'.'}".strip()
            result.append({"rank": r.get("rank", i+1), "display_name": name or "—",
                           "username": u.get("telegram_username"),
                           "total_earned": r.get("total_earned", 0),
                           "questions_correct": r.get("questions_correct", 0)})
        else:
            name = f"{r.get('first_name','')} {(r.get('last_name') or [''])[0]+'.'}".strip()
            result.append({"rank": i+1, "display_name": name or "—",
                           "username": r.get("telegram_username"),
                           "total_earned": r.get("total_earned", 0),
                           "games_played": r.get("games_played", 0),
                           "games_won": r.get("games_won", 0),
                           "best_streak": r.get("best_streak", 0)})
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# DASHBOARD STATS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/fix-rls")
async def fix_rls(_=Depends(require_admin)):
    """
    One-time: apply missing public RLS policy on game_questions so the mini-app
    can read which games have questions (needed for active-game display).
    Run once from Supabase SQL editor or call this endpoint:
      POST /api/admin/fix-rls  (with X-Admin-Token header)

    SQL to run manually in Supabase SQL editor if preferred:
      ALTER TABLE game_questions ENABLE ROW LEVEL SECURITY;
      DROP POLICY IF EXISTS game_questions_select ON game_questions;
      CREATE POLICY game_questions_select ON game_questions FOR SELECT USING (TRUE);
    """
    sb = get_sb()
    try:
        await run(lambda: sb.rpc("query", {}).execute())
    except Exception:
        pass
    return {
        "ok": True,
        "sql": (
            "ALTER TABLE game_questions ENABLE ROW LEVEL SECURITY; "
            "DROP POLICY IF EXISTS game_questions_select ON game_questions; "
            "CREATE POLICY game_questions_select ON game_questions FOR SELECT USING (TRUE);"
        ),
        "note": "Copy and run the SQL above in your Supabase SQL editor → https://supabase.com/dashboard"
    }

@app.get("/api/stats")
async def get_stats(_=Depends(require_admin)):
    sb = get_sb()
    try:
        def q_total_users():   return sb.table("users").select("id", count="exact").execute()
        def q_active_users():  return sb.table("users").select("id", count="exact").eq("is_active", True).eq("is_banned", False).execute()
        def q_banned_users():  return sb.table("users").select("id", count="exact").eq("is_banned", True).execute()
        def q_total_games():   return sb.table("games").select("id", count="exact").execute()
        def q_active_game():   return sb.table("games").select(
            "id,title,status,total_players,total_winners,total_paid_out,prize_pool_etb,prize_pool_remaining,game_date"
        ).eq("status", "active").limit(1).execute()
        def q_pend_withdraw(): return sb.table("withdrawals").select("id", count="exact").eq("status", "pending").execute()
        def q_active_comp():   return sb.table("companies").select("id", count="exact").eq("status", "active").execute()
        def q_pend_comp():     return sb.table("companies").select("id", count="exact").eq("status", "pending").execute()
        def q_pend_qs():       return sb.table("questions").select("id", count="exact").eq("status", "pending").execute()
        def q_total_comp():    return sb.table("companies").select("id", count="exact").execute()
        def q_total_qs():      return sb.table("questions").select("id", count="exact").execute()
        # FIX 1: get real commission income from company_deposits, not estimated from payouts
        def q_fee_income():    return sb.table("company_deposits").select("commission_etb").eq("status", "confirmed").execute()

        (r_total_users, r_active_users, r_banned_users, r_total_games,
         r_active_game, r_pend_withdraw, r_active_comp, r_pend_comp,
         r_pend_qs, r_total_comp, r_total_qs, r_fee_income) = await gather(
            q_total_users, q_active_users, q_banned_users, q_total_games,
            q_active_game, q_pend_withdraw, q_active_comp, q_pend_comp,
            q_pend_qs, q_total_comp, q_total_qs, q_fee_income
        )

        # FIX 1: sum actual commission_etb from confirmed deposits
        fee_income = sum(float(d.get("commission_etb") or 0) for d in (r_fee_income.data or []))
        active_game = (r_active_game.data or [None])[0]

        return {
            "total_users":         r_total_users.count or 0,
            "active_users":        r_active_users.count or 0,
            "banned_users":        r_banned_users.count or 0,
            "total_games":         r_total_games.count or 0,
            "active_game":         active_game,
            "pending_withdrawals": r_pend_withdraw.count or 0,
            "active_companies":    r_active_comp.count or 0,
            "pending_companies":   r_pend_comp.count or 0,
            "total_companies":     r_total_comp.count or 0,
            "pending_questions":   r_pend_qs.count or 0,
            "total_questions":     r_total_qs.count or 0,
            "platform_fee_income": fee_income,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Stats error: %s", e)
        raise HTTPException(500, str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# USERS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/users")
async def list_users(
    request: Request, _=Depends(require_admin),
    page: int = 1, per_page: int = 20,
    search: str = "", role: str = "", status: str = "",
    sort: str = "created_at", order: str = "desc"
):
    sb = get_sb()
    safe_sort = sort if sort in ("created_at","balance","total_earned","games_played","first_name") else "created_at"

    def _query():
        q = sb.table("users").select(
            "id,telegram_id,telegram_username,first_name,last_name,phone_number,photo_url,role,"
            "is_active,is_banned,ban_reason,balance,total_earned,total_withdrawn,"
            "games_played,games_won,correct_answers,wrong_answers,current_streak,best_streak,"
            "last_game_date,created_at",
            count="exact"
        )
        if search:
            q = q.or_(f"first_name.ilike.%{search}%,last_name.ilike.%{search}%,telegram_username.ilike.%{search}%,phone_number.ilike.%{search}%")
        if role:   q = q.eq("role", role)
        if status == "active":   q = q.eq("is_active", True).eq("is_banned", False)
        elif status == "banned": q = q.eq("is_banned", True)
        elif status == "inactive": q = q.eq("is_active", False)
        q = q.order(safe_sort, desc=(order.lower() == "desc"))
        offset = (page - 1) * per_page
        return q.range(offset, offset + per_page - 1).execute()

    res = await run(_query)
    return {"data": res.data or [], "total": res.count or 0, "page": page, "per_page": per_page}

@app.get("/api/users/{uid}")
async def get_user(uid: str, _=Depends(require_admin)):
    sb = get_sb()
    res = await run(lambda: sb.table("users").select("*").eq("id", uid).single().execute())
    if not res.data: raise HTTPException(404, "User not found")
    return res.data

@app.put("/api/users/{uid}")
async def update_user(uid: str, body: UserUpdate, _=Depends(require_admin)):
    updates = body.dict(exclude_none=True)
    if not updates: raise HTTPException(400, "Nothing to update")
    sb = get_sb()
    await run(lambda: sb.table("users").update(updates).eq("id", uid).execute())
    return await get_user(uid, _)

@app.delete("/api/users/{uid}")
async def delete_user(uid: str, _=Depends(require_admin)):
    """
    Delete a user. Clears all FK references (no CASCADE) in safe order, then deletes the row.
    Tables with ON DELETE CASCADE (game_sessions, leaderboard, withdrawals, notifications,
    companies via owner_id) are handled automatically by the DB after the user row is removed.
    """
    sb = get_sb()
    # Verify user exists
    chk = await run(lambda: sb.table("users").select("id,telegram_id").eq("id", uid).limit(1).execute())
    if not chk.data:
        raise HTTPException(404, "User not found")
    telegram_id = chk.data[0].get("telegram_id")

    # Find admin user to reassign NOT-NULL created_by FKs
    admin_res = await run(lambda: sb.table("users").select("id").eq("role", "admin").neq("id", uid).limit(1).execute())
    admin_id = (admin_res.data or [{}])[0].get("id") if admin_res.data else None

    def _cascade_delete():
        # 1. game_sessions CASCADE → round_answers + spin_results deleted automatically
        sb.table("game_sessions").delete().eq("user_id", uid).execute()
        # 2. transactions — REFERENCES users(id) NO CASCADE
        sb.table("transactions").delete().eq("user_id", uid).execute()
        # 3. round_answers + spin_results that may survive (user_id not cascade from users)
        sb.table("round_answers").delete().eq("user_id", uid).execute()
        sb.table("spin_results").delete().eq("user_id", uid).execute()
        # 4. qr_scans — nullable user_id reference, NO CASCADE
        sb.table("qr_scans").delete().eq("user_id", uid).execute()
        if telegram_id:
            sb.table("qr_scans").delete().eq("telegram_id", telegram_id).execute()
        # 5. leaderboard, withdrawals, notifications (CASCADE from users, but explicit)
        sb.table("leaderboard").delete().eq("user_id", uid).execute()
        sb.table("withdrawals").delete().eq("user_id", uid).execute()
        sb.table("notifications").delete().eq("user_id", uid).execute()
        # 6. Reassign NOT NULL FK columns to admin (cannot be set to NULL)
        if admin_id:
            try:
                sb.table("questions").update({"created_by": admin_id}).eq("created_by", uid).execute()
            except Exception: pass
            try:
                sb.table("games").update({"created_by": admin_id}).eq("created_by", uid).execute()
            except Exception: pass
            try:
                sb.table("qr_codes").update({"created_by": admin_id}).eq("created_by", uid).execute()
            except Exception: pass
        else:
            # No other admin → delete questions/games/qr_codes created by this user
            try:
                sb.table("game_questions").delete().in_(
                    "question_id",
                    [r["id"] for r in (sb.table("questions").select("id").eq("created_by", uid).execute().data or [])]
                ).execute()
                sb.table("questions").delete().eq("created_by", uid).execute()
            except Exception: pass
        # 7. Nullable FK columns — NULL out
        try:
            sb.table("questions").update({"reviewed_by": None}).eq("reviewed_by", uid).execute()
        except Exception: pass
        try:
            sb.table("qr_codes").update({"revoked_by": None}).eq("revoked_by", uid).execute()
        except Exception: pass
        try:
            sb.table("company_deposits").update({"confirmed_by": None}).eq("confirmed_by", uid).execute()
        except Exception: pass
        try:
            sb.table("withdrawals").update({"processed_by": None}).eq("processed_by", uid).execute()
        except Exception: pass
        try:
            sb.table("audit_log").update({"actor_id": None}).eq("actor_id", uid).execute()
        except Exception: pass
        # 8. Companies owned by user (CASCADE → also removes company_deposits, games SET NULL)
        sb.table("companies").delete().eq("owner_id", uid).execute()
        # 9. Finally delete the user row
        sb.table("users").delete().eq("id", uid).execute()

    try:
        await run(_cascade_delete)
        logger.info("delete_user: uid=%s", uid)
        return {"message": "User deleted"}
    except Exception as e:
        logger.error("delete_user %s: %s", uid, e)
        raise HTTPException(500, str(e))

@app.post("/api/users/{uid}/ban")
async def ban_user(uid: str, _=Depends(require_admin)):
    sb = get_sb()
    res = await run(lambda: sb.table("users").select("is_banned").eq("id", uid).single().execute())
    if not res.data: raise HTTPException(404, "User not found")
    new_val = not res.data["is_banned"]
    await run(lambda: sb.table("users").update({"is_banned": new_val}).eq("id", uid).execute())
    return {"is_banned": new_val, "message": "Banned" if new_val else "Unbanned"}

@app.post("/api/users/{uid}/balance")
async def adjust_balance(uid: str, body: BalanceAdjust, _=Depends(require_admin)):
    sb = get_sb()
    res = await run(lambda: sb.table("users").select("balance").eq("id", uid).single().execute())
    if not res.data: raise HTTPException(404, "User not found")
    bal_before = float(res.data["balance"] or 0)
    if body.type == "credit":
        bal_after = bal_before + body.amount
        await run(lambda: sb.table("users").update({"balance": bal_after, "total_earned": bal_after}).eq("id", uid).execute())
    else:
        bal_after = max(0, bal_before - body.amount)
        await run(lambda: sb.table("users").update({"balance": bal_after}).eq("id", uid).execute())
    await run(lambda: sb.table("transactions").insert({
        "user_id": uid,
        "type": "admin_credit" if body.type == "credit" else "admin_debit",
        "amount": body.amount if body.type == "credit" else -body.amount,
        "balance_before": bal_before, "balance_after": bal_after,
        "description": body.note or "Admin adjustment"
    }).execute())
    return {"balance_before": bal_before, "balance_after": bal_after}


class ClearGameTestReq(BaseModel):
    """Optional game_id: if set, clear only that game's scan/session; else clear all for user."""
    game_id: Optional[str] = None


@app.post("/api/users/{uid}/clear-game-test")
async def clear_user_game_test(uid: str, body: ClearGameTestReq = None, _=Depends(require_admin)):
    """
    Clear ALL game-related data for a user so they can re-enter and test again (no "Already played").
    Body: {} or {"game_id": "<uuid>"}.
    Deletes in order:
      1. qr_scans (so QR scan allows re-entry)
      2. round_answers (via session delete or explicit)
      3. spin_results (via session delete or explicit)
      4. leaderboard rows for this user
      5. game_sessions (cascade removes round_answers + spin_results if not already gone)
    After this, server-side "Already played" is cleared. If the mini-app still shows it,
    the player must clear site data / storage for the app (localStorage cache).
    """
    sb = get_sb()
    body = body or ClearGameTestReq()
    game_id = (body.game_id or "").strip() or None
    try:
        # Get user's telegram_id so we clear scans by both user_id and telegram_id (mini-app checks both)
        user_row = await run(lambda: sb.table("users").select("telegram_id").eq("id", uid).limit(1).execute())
        telegram_id = (user_row.data or [{}])[0].get("telegram_id") if user_row.data else None

        # 1) qr_scans — by user_id and by telegram_id (removes "already entered" from QR flow)
        q_scans = sb.table("qr_scans").delete().eq("user_id", uid)
        if game_id:
            q_scans = q_scans.eq("game_id", game_id)
        await run(lambda: q_scans.execute())
        if telegram_id is not None:
            q_scans_tg = sb.table("qr_scans").delete().eq("telegram_id", telegram_id)
            if game_id:
                q_scans_tg = q_scans_tg.eq("game_id", game_id)
            await run(lambda: q_scans_tg.execute())

        # 2) leaderboard — user's rank/earned for this game (no FK to sessions)
        q_lb = sb.table("leaderboard").delete().eq("user_id", uid)
        if game_id:
            q_lb = q_lb.eq("game_id", game_id)
        await run(lambda: q_lb.execute())

        # 3) game_sessions — CASCADE deletes round_answers + spin_results
        q_sess = sb.table("game_sessions").delete().eq("user_id", uid)
        if game_id:
            q_sess = q_sess.eq("game_id", game_id)
        await run(lambda: q_sess.execute())

        scope = f"game {game_id}" if game_id else "all games"
        logger.info("clear_game_test: user=%s scope=%s", uid, scope)
        return {
            "ok": True,
            "message": "All game cache cleared for this user. They can scan QR and play again. If the app still shows ‘Already played’, the player must clear the app’s site data (localStorage).",
        }
    except Exception as e:
        logger.warning("clear_game_test %s: %s", uid, e)
        raise HTTPException(500, str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# GAMES
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/games")
async def list_games(
    request: Request, _=Depends(require_admin),
    page: int = 1, per_page: int = 20, status: str = "", search: str = ""
):
    sb = get_sb()
    def _query():
        q = sb.table("games").select("*, companies(name)", count="exact")
        if status: q = q.eq("status", status)
        if search: q = q.ilike("title", f"%{search}%")
        offset = (page - 1) * per_page
        return q.order("created_at", desc=True).range(offset, offset + per_page - 1).execute()
    res = await run(_query)
    games = res.data or []
    for g in games:
        g["company_name"] = (g.pop("companies", None) or {}).get("name") or g.get("company_name")
    return {"data": games, "total": res.count or 0, "page": page, "per_page": per_page}

@app.get("/api/games/{gid}")
async def get_game(gid: str, _=Depends(require_admin)):
    sb = get_sb()
    res = await run(lambda: sb.table("games").select("*, companies(name, id)").eq("id", gid).limit(1).execute())
    rows = res.data or []
    if not rows: raise HTTPException(404, "Game not found")
    g = rows[0]
    g["company_name"] = (g.pop("companies", None) or {}).get("name")
    return g

@app.post("/api/games")
async def create_game(body: GameCreate, _=Depends(require_admin)):
    sb = get_sb()
    admin = await run(lambda: sb.table("users").select("id").eq("role", "admin").limit(1).execute())
    created_by = (admin.data or [{}])[0].get("id")
    payload = body.dict()
    payload["created_by"] = created_by
    payload["company_id"] = payload["company_id"] or None
    payload["prize_pool_remaining"] = payload["prize_pool_etb"]
    res = await run(lambda: sb.table("games").insert(payload).execute())
    game_data = (res.data or [{}])[0]
    # Notification center: we notify users on QR generation only, not on game creation
    return game_data

@app.put("/api/games/{gid}")
async def update_game(gid: str, body: GameUpdate, _=Depends(require_admin)):
    updates = body.dict(exclude_none=True)
    if not updates: raise HTTPException(400, "Nothing to update")
    sb = get_sb()
    await run(lambda: sb.table("games").update(updates).eq("id", gid).execute())
    return await get_game(gid, _)

@app.delete("/api/games/{gid}")
async def delete_game(gid: str, _=Depends(require_admin)):
    """Delete a game after removing rows that reference it without ON DELETE CASCADE."""
    sb = get_sb()

    def _delete_game_cascade():
        # Tables that reference games(id) without CASCADE — delete in dependency order
        sb.table("qr_scans").delete().eq("game_id", gid).execute()
        sb.table("round_answers").delete().eq("game_id", gid).execute()
        sb.table("spin_results").delete().eq("game_id", gid).execute()
        # Now delete game (CASCADE will remove game_sessions, game_questions, qr_codes, leaderboard)
        sb.table("games").delete().eq("id", gid).execute()

    await run(_delete_game_cascade)
    return {"message": "Game deleted"}

@app.post("/api/games/{gid}/activate")
async def activate_game(gid: str, _=Depends(require_admin)):
    """
    Go live: requires at least one question AND one active QR code.
    Uses service key directly (bypasses RLS so game_questions is always readable).
    """
    sb = get_sb()
    # Use count queries (more reliable than data checks)
    gq_res  = await run(lambda: sb.table("game_questions").select("id", count="exact").eq("game_id", gid).execute())
    qr_res  = await run(lambda: sb.table("qr_codes").select("id", count="exact").eq("game_id", gid).eq("status", "active").execute())
    gq_count = gq_res.count or 0
    qr_count = qr_res.count or 0
    if gq_count == 0:
        raise HTTPException(400, f"No questions found for this game (found 0). Add questions first (Questions page → bulk-assign to game).")
    if qr_count == 0:
        raise HTTPException(400, f"No active QR codes found for this game. Generate at least one QR code in QR Manager.")
    await run(lambda: sb.table("games").update({"status": "active"}).eq("id", gid).execute())
    logger.info("activate_game: gid=%s questions=%d qr_codes=%d", gid, gq_count, qr_count)
    # Notification center: we notify users on QR generation only, not on game activation
    return {"status": "active", "questions": gq_count, "qr_codes": qr_count}

@app.post("/api/games/{gid}/end")
async def end_game(gid: str, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("games").update({"status": "ended"}).eq("id", gid).execute())
    return {"status": "ended"}


@app.post("/api/admin/broadcast-game/{game_id}")
async def admin_broadcast_game(game_id: str, _=Depends(require_admin)):
    """
    Manually trigger broadcast for a game. Fetches game + company from DB and sends
    notification to all users with notifications_enabled.
    """
    sb = get_sb()
    res = await run(lambda: sb.table("games").select("*").eq("id", game_id).single().execute())
    if not res.data:
        raise HTTPException(404, "Game not found")
    game_with_company = await _enrich_game_for_broadcast(sb, res.data)
    try:
        from bot import get_bot_app, broadcast_new_game
        app = get_bot_app()
        if app and app.bot:
            stats = await broadcast_new_game(game_with_company)
            return {"message": "Broadcast started", "game_id": game_id, "stats": stats}
        return {"message": "Broadcast started", "game_id": game_id, "stats": None, "warning": "Bot not available"}
    except Exception as e:
        logger.error("Admin broadcast failed: %s", e)
        raise HTTPException(500, str(e))

@app.get("/api/games/{gid}/questions")
async def get_game_questions_admin(gid: str, _=Depends(require_admin)):
    """Admin: returns questions with is_correct visible."""
    sb = get_sb()
    gq_res = await run(lambda: sb.table("game_questions").select(
        "sort_order, questions(id, icon, question_text, category, status, explanation,"
        "answer_options(id, option_letter, option_text, is_correct, sort_order))"
    ).eq("game_id", gid).order("sort_order").execute())
    rows = gq_res.data or []
    result = []
    for row in rows:
        q = row.get("questions") or {}
        q["sort_order"] = row.get("sort_order", 0)
        q["options"] = sorted(q.pop("answer_options", []) or [], key=lambda x: x.get("sort_order", 0))
        result.append(q)
    return result

@app.delete("/api/games/{gid}/questions/{qid}")
async def remove_question_from_game(gid: str, qid: str, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("game_questions").delete()
        .eq("game_id", gid).eq("question_id", qid).execute())
    return {"message": "Question removed from game"}

# ═══════════════════════════════════════════════════════════════════════════════
# QUESTIONS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/questions")
async def list_questions(
    request: Request, _=Depends(require_admin),
    page: int = 1, per_page: int = 20,
    status: str = "", search: str = "", game_id: str = ""
):
    sb = get_sb()
    def _qs():
        q = sb.table("questions").select("*", count="exact")
        if status: q = q.eq("status", status)
        if search: q = q.ilike("question_text", f"%{search}%")
        offset = (page - 1) * per_page
        return q.order("created_at", desc=True).range(offset, offset + per_page - 1).execute()

    res = await run(_qs)
    questions = res.data or []

    if game_id and questions:
        gq_res = await run(lambda: sb.table("game_questions").select("question_id").eq("game_id", game_id).execute())
        linked_ids = {row["question_id"] for row in (gq_res.data or [])}
        questions = [q for q in questions if q["id"] in linked_ids]

    if questions:
        qids = [q["id"] for q in questions]
        opts_res = await run(lambda: sb.table("answer_options").select("*").in_("question_id", qids).order("sort_order").execute())
        opts_map: Dict[str, list] = {}
        for opt in (opts_res.data or []):
            opts_map.setdefault(opt["question_id"], []).append(opt)

        gq_all = await run(lambda: sb.table("game_questions").select("question_id, games(id, title)").in_("question_id", qids).execute())
        gq_map: Dict[str, list] = {}
        for row in (gq_all.data or []):
            gq_map.setdefault(row["question_id"], []).append((row.get("games") or {}).get("title", "?"))

        for q in questions:
            q["options"] = opts_map.get(q["id"], [])
            q["games"]   = gq_map.get(q["id"], [])

    return {"data": questions, "total": res.count or 0, "page": page, "per_page": per_page}

@app.get("/api/questions/{qid}")
async def get_question(qid: str, _=Depends(require_admin)):
    sb = get_sb()
    res_q, res_opts = await gather(
        lambda: sb.table("questions").select("*").eq("id", qid).single().execute(),
        lambda: sb.table("answer_options").select("*").eq("question_id", qid).order("sort_order").execute(),
    )
    if not res_q.data: raise HTTPException(404, "Question not found")
    q = res_q.data
    q["options"] = res_opts.data or []
    return q

@app.post("/api/questions")
async def create_question(body: QuestionCreate, _=Depends(require_admin)):
    sb = get_sb()
    admin = await run(lambda: sb.table("users").select("id").eq("role", "admin").limit(1).execute())
    created_by = (admin.data or [{}])[0].get("id")
    payload = {
        "question_text": body.question_text, "category": body.category,
        "explanation": body.explanation, "icon": body.icon,
        "company_id": body.company_id, "is_sponsored": body.is_sponsored,
        "created_by": created_by, "status": "approved"
    }
    qrow_res = await run(lambda: sb.table("questions").insert(payload).execute())
    qrow = (qrow_res.data or [{}])[0]
    qid = qrow["id"]

    opts = [{"question_id": qid, "option_letter": opt["letter"],
             "option_text": opt["text"], "is_correct": opt.get("is_correct", False),
             "sort_order": ord(opt["letter"]) - ord("A")} for opt in body.options]
    if opts:
        await run(lambda: sb.table("answer_options").insert(opts).execute())

    if body.game_id:
        gq_res = await run(lambda: sb.table("game_questions").select("id")
            .eq("game_id", body.game_id).eq("question_id", qid).execute())
        if not gq_res.data:
            max_ord = await run(lambda: sb.table("game_questions").select("sort_order")
                .eq("game_id", body.game_id).order("sort_order", desc=True).limit(1).execute())
            next_ord = ((max_ord.data or [{}])[0].get("sort_order") or 0) + 1
            await run(lambda: sb.table("game_questions").insert({
                "game_id": body.game_id, "question_id": qid, "sort_order": next_ord
            }).execute())
        qrow["game_id"] = body.game_id
    return qrow


# ─── AI question generation (Anthropic Claude) ─────────────────────────────────
def _generate_questions_via_claude(
    categories: List[str],
    difficulty: str,
    count: int,
    language: str,
) -> List[Dict[str, Any]]:
    """Call Claude to generate Ethiopian trivia questions. Returns list of question dicts."""
    if not ANTHROPIC_API_KEY or not anthropic:
        raise HTTPException(503, "ANTHROPIC_API_KEY not set or anthropic package not installed")
    count = max(1, min(20, count))
    cat_list = ", ".join(categories) if categories else "Ethiopian History, Culture, Sports, Geography"
    diff_guide = {
        "Easy": "Basic knowledge that most Ethiopians would know (e.g. capital city, famous landmarks).",
        "Medium": "Requires some study or general awareness (recent events, notable figures, culture).",
        "Hard": "Expert level: deep knowledge, obscure facts, precise dates or statistics.",
    }
    diff_instruction = diff_guide.get(difficulty, diff_guide["Medium"])
    lang_instruction = "Output question_text and all option text in English only."
    if language == "Amharic":
        lang_instruction = "Output question_text and all option text in Amharic only (use Amharic script)."
    elif language == "Both":
        lang_instruction = (
            "Output each question with two fields: question_text (English) and question_text_amharic (Amharic). "
            "Options: use option_text for English and option_text_amharic for Amharic. "
            "Generate both versions for every question."
        )

    system = (
        "You are an expert Ethiopian trivia writer. Today's date is March 2, 2026. "
        "Focus on accurate, up-to-date information including: Adwa 130th anniversary (March 2 2026), "
        "Ethiopian sea access discussions, Tigray politics (e.g. Simret party), Ethiopian Premier League standings, "
        "Ethio Telecom (teleStream, Next Horizon 2028), and other recent 2025–2026 events where relevant. "
        "Return ONLY a valid JSON array. No markdown, no code fences, no explanation outside the JSON. "
        "Each object in the array must have: "
        '"question_text" (string), '
        '"options" (array of exactly 4 objects, each with "letter" (A/B/C/D), "text" (string), "is_correct" (boolean, exactly one true)), '
        '"explanation" (string, optional), "category" (string, optional). '
        "If language is Both, also include question_text_amharic and per-option option_text_amharic."
    )
    user = (
        f"Generate exactly {count} multiple-choice quiz questions. "
        f"Categories to cover: {cat_list}. "
        f"Difficulty: {difficulty}. {diff_instruction} "
        f"Language: {lang_instruction} "
        "Return a JSON array only."
    )

    # Long timeout: generation can take 60–120s; proxies often drop shorter connections
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=180.0)
    try:
        msg = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=8192,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
    except (_httpx.RemoteProtocolError, _httpx.ConnectError) as e:
        raise HTTPException(
            502,
            "Connection to Claude failed (network or proxy dropped). Try again, use fewer questions (e.g. 5), or disable proxy for this app."
        ) from e
    except _httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(
                502,
                "Claude model not found (404). Set ANTHROPIC_MODEL in .env to a valid model id (e.g. claude-opus-4-6)."
            ) from e
        raise HTTPException(502, f"Claude API error: {e.response.status_code}") from e
    except Exception as e:
        if type(e).__name__ == "APIConnectionError" or "connection" in str(e).lower() or "disconnected" in str(e).lower():
            raise HTTPException(
                502,
                "Connection to Claude failed (network or proxy dropped). Try again, use fewer questions (e.g. 5), or disable proxy for this app."
            ) from e
        if getattr(e, "response", None) and getattr(e.response, "status_code", None) == 404:
            raise HTTPException(502, "Claude model not found (404). Set ANTHROPIC_MODEL in .env to a valid model id (e.g. claude-opus-4-6).") from e
        if "404" in str(e) or "not_found" in str(e).lower():
            raise HTTPException(502, "Claude model not found (404). Set ANTHROPIC_MODEL in .env to a valid model id (e.g. claude-opus-4-6).") from e
        raise
    text = ""
    for block in (msg.content or []):
        if getattr(block, "text", None):
            text += block.text
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        raise HTTPException(502, f"Claude returned invalid JSON: {e}")
    if not isinstance(raw, list):
        raw = [raw] if isinstance(raw, dict) else []

    result = []
    for item in raw[:count]:
        qtext = (item.get("question_text") or item.get("question") or "").strip()
        if not qtext:
            continue
        opts_raw = item.get("options") or []
        opts = []
        for i, o in enumerate(opts_raw[:4]):
            if isinstance(o, str):
                opts.append({"letter": "ABCD"[i], "text": o.strip(), "is_correct": i == 0})
            else:
                letter = str(o.get("letter", "ABCD"[i]))[0].upper()
                if letter not in "ABCD":
                    letter = "ABCD"[i]
                opts.append({
                    "letter": letter,
                    "text": (str(o.get("text") or o.get("option_text") or "").strip()) or f"Option {letter}",
                    "is_correct": bool(o.get("is_correct")),
                })
        if len(opts) < 2:
            continue
        if not any(x["is_correct"] for x in opts):
            opts[0]["is_correct"] = True
        result.append({
            "question_text": qtext,
            "options": opts,
            "category": (item.get("category") or "").strip() or None,
            "explanation": (item.get("explanation") or "").strip() or None,
        })
    return result


def _build_ai_prompts(
    categories: List[str],
    difficulty: str,
    count: int,
    language: str,
) -> tuple:
    """Build system and user prompt text for question generation (shared by OpenAI and Claude)."""
    count = max(1, min(20, count))
    cat_list = ", ".join(categories) if categories else "Ethiopian History, Culture, Sports, Geography"
    diff_guide = {
        "Easy": "Basic knowledge that most Ethiopians would know (e.g. capital city, famous landmarks).",
        "Medium": "Requires some study or general awareness (recent events, notable figures, culture).",
        "Hard": "Expert level: deep knowledge, obscure facts, precise dates or statistics.",
    }
    diff_instruction = diff_guide.get(difficulty, diff_guide["Medium"])
    lang_instruction = "Output question_text and all option text in English only."
    if language == "Amharic":
        lang_instruction = "Output question_text and all option text in Amharic only (use Amharic script)."
    elif language == "Both":
        lang_instruction = (
            "Output each question with two fields: question_text (English) and question_text_amharic (Amharic). "
            "Options: use option_text for English and option_text_amharic for Amharic. Generate both versions."
        )
    system = (
        "You are an expert Ethiopian trivia writer. Today's date is March 2, 2026. "
        "Focus on accurate, up-to-date information including: Adwa 130th anniversary (March 2 2026), "
        "Ethiopian sea access discussions, Tigray politics (e.g. Simret party), Ethiopian Premier League standings, "
        "Ethio Telecom (teleStream, Next Horizon 2028), and other recent 2025–2026 events where relevant. "
        "Return ONLY a valid JSON array. No markdown, no code fences, no explanation outside the JSON. "
        "Each object in the array must have: "
        '"question_text" (string), '
        '"options" (array of exactly 4 objects, each with "letter" (A/B/C/D), "text" (string), "is_correct" (boolean, exactly one true)), '
        '"explanation" (string, optional), "category" (string, optional).'
    )
    user = (
        f"Generate exactly {count} multiple-choice quiz questions. "
        f"Categories to cover: {cat_list}. "
        f"Difficulty: {difficulty}. {diff_instruction} "
        f"Language: {lang_instruction} "
        "Return a JSON array only."
    )
    return system, user, count


def _parse_ai_questions_json(text: str, count: int) -> List[Dict[str, Any]]:
    """Parse LLM JSON response into list of question dicts (shared by OpenAI and Claude)."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        raise HTTPException(502, f"AI returned invalid JSON: {e}")
    if not isinstance(raw, list):
        raw = [raw] if isinstance(raw, dict) else []
    result = []
    for item in raw[:count]:
        qtext = (item.get("question_text") or item.get("question") or "").strip()
        if not qtext:
            continue
        opts_raw = item.get("options") or []
        opts = []
        for i, o in enumerate(opts_raw[:4]):
            if isinstance(o, str):
                opts.append({"letter": "ABCD"[i], "text": o.strip(), "is_correct": i == 0})
            else:
                letter = str(o.get("letter", "ABCD"[i]))[0].upper()
                if letter not in "ABCD":
                    letter = "ABCD"[i]
                opts.append({
                    "letter": letter,
                    "text": (str(o.get("text") or o.get("option_text") or "").strip()) or f"Option {letter}",
                    "is_correct": bool(o.get("is_correct")),
                })
        if len(opts) < 2:
            continue
        if not any(x["is_correct"] for x in opts):
            opts[0]["is_correct"] = True
        result.append({
            "question_text": qtext,
            "options": opts,
            "category": (item.get("category") or "").strip() or None,
            "explanation": (item.get("explanation") or "").strip() or None,
        })
    return result


@app.post("/api/admin/generate-questions")
async def generate_questions_admin(body: GenerateQuestionsReq, _=Depends(require_admin)):
    """
    Generate quiz questions using AI (Anthropic Claude). Requires ANTHROPIC_API_KEY and ANTHROPIC_MODEL in .env.
    Saves to Supabase (questions + answer_options) with status=approved, created_by=admin.
    """
    sb = get_sb()
    admin = await run(lambda: sb.table("users").select("id").eq("role", "admin").limit(1).execute())
    created_by = (admin.data or [{}])[0].get("id")
    if not created_by:
        raise HTTPException(500, "No admin user found for created_by")

    loop = asyncio.get_event_loop()
    questions = await loop.run_in_executor(
        None,
        lambda: _generate_questions_via_claude(
            body.categories or [],
            body.difficulty,
            body.count,
            body.language or "English",
        ),
    )

    saved = 0
    ai_prompt_summary = f"Categories: {', '.join(body.categories) or 'General'}. Difficulty: {body.difficulty}. Language: {body.language or 'English'}."
    for q in questions:
        try:
            payload = {
                "question_text": q["question_text"],
                "category": ", ".join(body.categories) if body.categories else (q.get("category") or "AI Generated"),
                "explanation": q.get("explanation"),
                "icon": "🤖",
                "created_by": created_by,
                "status": "approved",
                "source": "AI Generated",
                "language": body.language or "English",
                "ai_prompt": ai_prompt_summary,
            }
            qrow_res = await run(lambda: sb.table("questions").insert(payload).execute())
            qrow = (qrow_res.data or [{}])[0]
            qid = qrow.get("id")
            if not qid:
                continue
            opts = [
                {
                    "question_id": qid,
                    "option_letter": opt["letter"],
                    "option_text": opt["text"],
                    "is_correct": opt.get("is_correct", False),
                    "sort_order": ord(opt["letter"]) - ord("A"),
                }
                for opt in q["options"]
            ]
            if opts:
                await run(lambda: sb.table("answer_options").insert(opts).execute())
            saved += 1
        except Exception as e:
            logger.warning("Failed to save AI question: %s", e)
            continue

    return {
        "message": f"{saved} questions generated and saved to database!",
        "count": saved,
    }


@app.post("/api/questions/bulk-delete")
async def bulk_delete_questions(body: BulkDeleteQuestionsReq, _=Depends(require_admin)):
    """Delete multiple questions; clear round_answers FK (no CASCADE) then delete."""
    ids = [x for x in (body.ids or []) if isinstance(x, str) and len(x) == 36]
    if not ids:
        raise HTTPException(400, "No valid question ids")
    sb = get_sb()
    def _bulk_delete():
        sb.table("round_answers").delete().in_("question_id", ids).execute()
        for qid in ids:
            sb.table("questions").delete().eq("id", qid).execute()
    await run(_bulk_delete)
    return {"message": f"Deleted {len(ids)} question(s)", "deleted": len(ids)}


@app.post("/api/questions/bulk-assign-game")
async def bulk_assign_questions_to_game(body: BulkAssignGameReq, _=Depends(require_admin)):
    """Link selected questions to a game (add game_questions rows). Skips if already linked."""
    qids = [x for x in (body.question_ids or []) if isinstance(x, str) and len(x) == 36]
    if not qids:
        raise HTTPException(400, "No valid question ids")
    if not (body.game_id or len(body.game_id) == 36):
        raise HTTPException(400, "Valid game_id required")
    sb = get_sb()
    existing = await run(lambda: sb.table("game_questions").select("question_id")
        .eq("game_id", body.game_id).in_("question_id", qids).execute())
    linked = {row["question_id"] for row in (existing.data or [])}
    max_ord = await run(lambda: sb.table("game_questions").select("sort_order")
        .eq("game_id", body.game_id).order("sort_order", desc=True).limit(1).execute())
    next_ord = ((max_ord.data or [{}])[0].get("sort_order") or 0) + 1
    added = 0
    for qid in qids:
        if qid in linked:
            continue
        await run(lambda qid=qid, no=next_ord: sb.table("game_questions").insert({
            "game_id": body.game_id, "question_id": qid, "sort_order": no
        }).execute())
        next_ord += 1
        added += 1
    return {"message": f"Assigned {added} question(s) to game", "added": added}


@app.put("/api/questions/{qid}")
async def update_question(qid: str, body: QuestionUpdate, _=Depends(require_admin)):
    sb = get_sb()
    updates = {k: v for k, v in body.dict(exclude_none=True).items() if k != "options"}
    if updates:
        await run(lambda: sb.table("questions").update(updates).eq("id", qid).execute())
    if body.options is not None:
        await run(lambda: sb.table("answer_options").delete().eq("question_id", qid).execute())
        allowed_letters = ("A", "B", "C", "D")
        opts = [
            {"question_id": qid, "option_letter": o["letter"], "option_text": o.get("text", ""),
             "is_correct": bool(o.get("is_correct")), "sort_order": ord(str(o["letter"])[0]) - ord("A")}
            for o in body.options if str(o.get("letter", ""))[:1] in allowed_letters
        ]
        if opts:
            await run(lambda: sb.table("answer_options").insert(opts).execute())
    return await get_question(qid, _)

@app.delete("/api/questions/{qid}")
async def delete_question(qid: str, _=Depends(require_admin)):
    """Delete question; clear round_answers FK first (no CASCADE on question_id)."""
    sb = get_sb()
    def _delete_q():
        sb.table("round_answers").delete().eq("question_id", qid).execute()
        sb.table("questions").delete().eq("id", qid).execute()
    await run(_delete_q)
    return {"message": "Question deleted"}


@app.post("/api/questions/{qid}/approve")
async def approve_question(qid: str, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("questions").update({
        "status": "approved", "reviewed_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", qid).execute())
    return {"status": "approved"}

@app.post("/api/questions/{qid}/reject")
async def reject_question(qid: str, body: RejectReq, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("questions").update({
        "status": "rejected", "rejected_reason": body.reason,
        "reviewed_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", qid).execute())
    return {"status": "rejected"}

# ═══════════════════════════════════════════════════════════════════════════════
# WITHDRAWALS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/withdrawals-summary")
async def withdrawals_summary(
    _=Depends(require_admin), status: str = "", search: str = ""
):
    """Aggregate totals for report: total_requested, total_fee, total_paid, total_count (same filters as list)."""
    sb = get_sb()
    empty = {"total_requested": 0, "total_fee": 0, "total_paid": 0, "total_count": 0}

    def _query():
        q = sb.table("withdrawals").select("amount_requested,fee_etb,amount_paid,phone_number")
        if status:
            q = q.eq("status", status)
        if search and search.strip():
            s = search.strip().replace(",", " ").replace("%", "")[:80]
            q = q.ilike("phone_number", f"%{s}%")
        return q.limit(10000).execute()

    try:
        res = await run(_query)
    except Exception as e:
        logger.warning("withdrawals_summary query failed: %s", e)
        return empty
    rows = res.data or []
    try:
        total_requested = sum(float(r.get("amount_requested") or 0) for r in rows)
        total_fee = sum(float(r.get("fee_etb") or 0) for r in rows)
        total_paid = sum(float(r.get("amount_paid") or 0) for r in rows)
        return {
            "total_requested": round(total_requested, 2),
            "total_fee": round(total_fee, 2),
            "total_paid": round(total_paid, 2),
            "total_count": len(rows),
        }
    except (TypeError, ValueError) as e:
        logger.warning("withdrawals_summary sum failed: %s", e)
        return empty


@app.get("/api/withdrawals")
async def list_withdrawals(
    request: Request, _=Depends(require_admin),
    page: int = 1, per_page: int = 20, status: str = "", search: str = "",
    include_summary: bool = False
):
    sb = get_sb()
    def _query():
        q = sb.table("withdrawals").select(
            "*, user:users!withdrawals_user_id_fkey(first_name,last_name,phone_number,telegram_username)",
            count="exact"
        )
        if status: q = q.eq("status", status)
        if search and search.strip():
            s = search.strip().replace(",", " ")[:80]  # avoid breaking or_ syntax
            q = q.or_(f"phone_number.ilike.%{s}%,chapa_reference.ilike.%{s}%")
        offset = (page - 1) * per_page
        return q.order("requested_at", desc=True).range(offset, offset + per_page - 1).execute()

    res = await run(_query)
    withdrawals = res.data or []
    for w in withdrawals:
        user = w.pop("user", None) or {}
        w["user_name"]         = f"{user.get('first_name','') or ''} {user.get('last_name','') or ''}".strip() or "—"
        w["user_phone"]        = user.get("phone_number")
        w["telegram_username"] = user.get("telegram_username")

    out = {"data": withdrawals, "total": res.count or 0, "page": page, "per_page": per_page}
    if include_summary:
        try:
            def _sum_query():
                q = sb.table("withdrawals").select("amount_requested,fee_etb,amount_paid")
                if status: q = q.eq("status", status)
                if search and search.strip():
                    s = search.strip().replace(",", " ").replace("%", "")[:80]
                    q = q.ilike("phone_number", f"%{s}%")
                return q.limit(10000).execute()
            sum_res = await run(_sum_query)
            rows = sum_res.data or []
            out["summary"] = {
                "total_requested": round(sum(float(r.get("amount_requested") or 0) for r in rows), 2),
                "total_fee": round(sum(float(r.get("fee_etb") or 0) for r in rows), 2),
                "total_paid": round(sum(float(r.get("amount_paid") or 0) for r in rows), 2),
                "total_count": len(rows),
            }
        except Exception as e:
            logger.warning("withdrawals list include_summary failed: %s", e)
            out["summary"] = {"total_requested": 0, "total_fee": 0, "total_paid": 0, "total_count": 0}
    return out

@app.get("/api/withdrawals/{wid}")
async def get_withdrawal(wid: str, _=Depends(require_admin)):
    if not _is_valid_uuid(wid):
        raise HTTPException(404, "Withdrawal not found")
    sb = get_sb()
    res = await run(lambda: sb.table("withdrawals").select(
        "*, user:users!withdrawals_user_id_fkey(first_name,last_name,phone_number,telegram_username)"
    ).eq("id", wid).single().execute())
    if not res.data: raise HTTPException(404, "Withdrawal not found")
    w = res.data
    user = w.pop("user", None) or {}
    w["user_name"]         = f"{user.get('first_name','') or ''} {user.get('last_name','') or ''}".strip() or "—"
    w["user_phone"]        = user.get("phone_number")
    w["telegram_username"] = user.get("telegram_username")
    return w

@app.put("/api/withdrawals/{wid}")
async def update_withdrawal(wid: str, body: WithdrawalUpdate, _=Depends(require_admin)):
    if body.notes is not None:
        sb = get_sb()
        await run(lambda: sb.table("withdrawals").update({"notes": body.notes}).eq("id", wid).execute())
    return await get_withdrawal(wid, _)

@app.post("/api/withdrawals/{wid}/approve")
async def approve_withdrawal(wid: str, _=Depends(require_admin)):
    """Approve: Admin manually sent money to Talabirr. Mark complete, update total_withdrawn."""
    sb = get_sb()
    now = datetime.now(timezone.utc).isoformat()
    try:
        w_res = await run(lambda: sb.table("withdrawals").select("*").eq("id", wid).eq("status", "pending").single().execute())
        if not w_res.data:
            raise HTTPException(404, "Withdrawal not found or not pending")
        w = w_res.data
        user_id = w["user_id"]
        amount_paid = float(w.get("amount_paid") or 0)

        # Update total_withdrawn
        u2 = await run(lambda: sb.table("users").select("total_withdrawn").eq("id", user_id).single().execute())
        tw = float((u2.data or {}).get("total_withdrawn") or 0)
        await run(lambda: sb.table("users").update({
            "total_withdrawn": round(tw + amount_paid, 2),
            "updated_at": now
        }).eq("id", user_id).execute())

        admin_res = await run(lambda: sb.table("users").select("id").eq("role", "admin").limit(1).execute())
        aid = (admin_res.data or [{}])[0].get("id") if admin_res.data else None
        upd = {"status": "completed", "processed_at": now}
        if aid:
            upd["processed_by"] = aid
        r_upd = await run(lambda: sb.table("withdrawals").update(upd).eq("id", wid).eq("status", "pending").execute())
        if not (getattr(r_upd, "data", None) and len(r_upd.data) > 0):
            raise HTTPException(409, "Withdrawal was already processed by another admin")
        # DB trigger on_withdrawal_status_change marks spin_results.w-status='deactive' atomically

        return {"status": "completed", "message": "Released to Talabirr"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("approve_withdrawal failed: %s", e)
        raise HTTPException(500, f"Withdrawal approve failed: {str(e)}")

@app.post("/api/withdrawals/{wid}/complete")
async def complete_withdrawal(wid: str, _=Depends(require_admin)):
    """Mark as completed (for withdrawals already in processing)."""
    sb = get_sb()
    now = datetime.now(timezone.utc).isoformat()
    try:
        w_res = await run(lambda: sb.table("withdrawals").select("user_id,amount_paid").eq("id", wid).eq("status", "processing").single().execute())
        if not w_res.data:
            raise HTTPException(404, "Withdrawal not found or not in processing")
        user_id = w_res.data["user_id"]
        amount_paid = float(w_res.data.get("amount_paid") or 0)
        u2 = await run(lambda: sb.table("users").select("total_withdrawn").eq("id", user_id).single().execute())
        tw = float((u2.data or {}).get("total_withdrawn") or 0)
        await run(lambda: sb.table("users").update({"total_withdrawn": round(tw + amount_paid, 2), "updated_at": now}).eq("id", user_id).execute())
        r = await run(lambda: sb.table("withdrawals").update({"status": "completed", "processed_at": now}).eq("id", wid).eq("status", "processing").execute())
        if not (getattr(r, "data", None) or len(r.data) > 0):
            raise HTTPException(404, "Withdrawal not found or not in processing")
        # DB trigger on_withdrawal_status_change marks spin_results.w-status='deactive' atomically

        return {"status": "completed"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("complete_withdrawal failed")
        raise HTTPException(500, f"Withdrawal complete failed: {str(e)}")

@app.post("/api/withdrawals/{wid}/deny")
async def deny_withdrawal(wid: str, body: Optional[DenyReq] = Body(default=None), _=Depends(require_admin)):
    """Deny: Refund reserved balance to user. Works for pending or processing."""
    sb = get_sb()
    reason = (body.reason if body else None) or "Admin denied"
    try:
        w_res = await run(lambda: sb.table("withdrawals").select("*").eq("id", wid).in_("status", ["pending", "processing"]).single().execute())
        if not w_res.data:
            raise HTTPException(404, "Withdrawal not found or not pending/processing")
        w = w_res.data
        user_id = w["user_id"]

        # Trigger on_withdrawal_status_change refunds when status→failed (migration 010)
        r = await run(lambda: sb.table("withdrawals").update({
            "status": "failed", "failure_reason": reason
        }).eq("id", wid).in_("status", ["pending", "processing"]).execute())
        if not (getattr(r, "data", None) or len(r.data) > 0):
            raise HTTPException(404, "Withdrawal not found or not pending/processing")
        return {"status": "failed"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("deny_withdrawal failed")
        raise HTTPException(500, f"Withdrawal deny failed: {str(e)}")

# ═══════════════════════════════════════════════════════════════════════════════
# PLAYER WITHDRAW (public — mini-app submits)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/player/withdraw/config")
async def withdraw_config():
    """Public: min amount, fee %, for mini-app to show withdrawal breakdown."""
    sb = get_sb()
    cfg = await run(lambda: sb.table("platform_config").select("key,value")
        .in_("key", ["withdrawal_fee_pct", "min_withdrawal_etb"]).execute())
    cfg_map = {r["key"]: r["value"] for r in (cfg.data or [])}
    return {
        "min_withdrawal_etb": float(cfg_map.get("min_withdrawal_etb", 50)),
        "withdrawal_fee_pct": float(cfg_map.get("withdrawal_fee_pct", 5)),
    }

def _normalize_phone(raw: str) -> str:
    """Normalize phone to +251XXXXXXXXX."""
    digits = "".join(c for c in (raw or "").strip() if c.isdigit())
    if not digits or len(digits) < 9:
        return ""
    if digits.startswith("0"):
        digits = "251" + digits[1:]
    elif not digits.startswith("251") and len(digits) == 9:
        digits = "251" + digits
    return "+" + digits

@app.post("/api/player/withdraw")
async def player_withdraw(body: WithdrawReq):
    """User requests withdrawal. Min 50 ETB, 5% fee. Phone must match registered. Balance reserved immediately."""
    sb = get_sb()
    cfg = await run(lambda: sb.table("platform_config").select("key,value")
        .in_("key", ["withdrawal_fee_pct","min_withdrawal_etb"]).execute())
    cfg_map = {r["key"]: r["value"] for r in (cfg.data or [])}
    fee_pct = float(cfg_map.get("withdrawal_fee_pct", 5))
    min_etb = float(cfg_map.get("min_withdrawal_etb", 50))

    if body.amount_requested < min_etb:
        raise HTTPException(400, f"Minimum withdrawal is {min_etb} ETB")
    if not (body.full_name or "").strip():
        raise HTTPException(400, "Full name (Telebirr registered) is required")

    u_res = await run(lambda: sb.table("users").select("phone_number").eq("id", body.user_id).single().execute())
    if not u_res.data: raise HTTPException(404, "User not found")
    # Available balance: same as profile — SUM(amount_etb) from spin_results WHERE w-status='active'
    balance = await _get_active_spin_balance(sb, body.user_id)
    reg_phone = _normalize_phone(u_res.data.get("phone_number") or "")
    if not reg_phone:
        raise HTTPException(400, "Share your phone number first (Account section)")
    amt = float(body.amount_requested)
    if round(balance, 2) < round(amt, 2):
        raise HTTPException(400, f"Insufficient balance ({balance:.2f} ETB available, {amt:.2f} requested)")

    # Phone must match registered (Telegram) phone
    req_phone = _normalize_phone(body.phone_number or "")
    if req_phone != reg_phone:
        raise HTTPException(400, "This number and Telegram number must match")

    # Block if pending or processing withdrawal exists (one at a time)
    pend = await run(lambda: sb.table("withdrawals").select("id").eq("user_id", body.user_id).in_("status", ["pending", "processing"]).limit(1).execute())
    if pend.data:
        raise HTTPException(400, "You already have a withdrawal in progress. Wait until it is completed.")

    # 5% fee, rest sent to user. e.g. 138 requested → 7 fee → 131 paid
    fee_etb    = round(body.amount_requested * fee_pct / 100, 0)
    amount_paid = round(body.amount_requested - fee_etb, 2)

    # Reserve balance immediately (deduct)
    bal_after = balance - body.amount_requested
    await run(lambda: sb.table("users").update({
        "balance": bal_after,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", body.user_id).execute())

    payload = {
        "user_id": body.user_id, "amount_requested": body.amount_requested,
        "fee_pct": fee_pct, "fee_etb": fee_etb, "amount_paid": amount_paid,
        "phone_number": req_phone, "full_name": (body.full_name or "").strip(),
        "bank_account": body.bank_account, "status": "pending"
    }
    res = await run(lambda: sb.table("withdrawals").insert(payload).execute())
    return (res.data or [{}])[0]

# ═══════════════════════════════════════════════════════════════════════════════
# COMPANIES
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/companies")
async def list_companies(
    request: Request, _=Depends(require_admin),
    page: int = 1, per_page: int = 20, status: str = "", search: str = ""
):
    sb = get_sb()
    def _query():
        q = sb.table("companies").select("*", count="exact")
        if status: q = q.eq("status", status)
        if search: q = q.ilike("name", f"%{search}%")
        offset = (page - 1) * per_page
        return q.order("created_at", desc=True).range(offset, offset + per_page - 1).execute()
    res = await run(_query)
    return {"data": res.data or [], "total": res.count or 0, "page": page, "per_page": per_page}

@app.get("/api/companies/{cid}")
async def get_company(cid: str, _=Depends(require_admin)):
    sb = get_sb()
    res = await run(lambda: sb.table("companies").select("*").eq("id", cid).single().execute())
    if not res.data: raise HTTPException(404, "Company not found")
    return res.data

@app.post("/api/companies")
async def create_company(body: CompanyCreate, _=Depends(require_admin)):
    sb = get_sb()
    owner_id = body.owner_id
    if not owner_id:
        admin = await run(lambda: sb.table("users").select("id").eq("role", "admin").limit(1).execute())
        owner_id = (admin.data or [{}])[0].get("id")
    payload = {
        "name": body.name,
        "slug": body.slug,
        "category": body.category,
        "description": body.description,
        "contact_email": body.contact_email,
        "contact_phone": body.contact_phone,
        "website": body.website,
        "logo_url": body.logo_url,
        "primary_color": body.primary_color or "#E8B84B",
        "owner_id": owner_id,
        "status": "pending",
    }
    res = await run(lambda: sb.table("companies").insert(payload).execute())
    return (res.data or [{}])[0]

@app.put("/api/companies/{cid}")
async def update_company(cid: str, body: CompanyUpdate, request: Request, _=Depends(require_admin)):
    """Update company. Handles logo_url and primary_color even when sent as null/empty."""
    raw_body = await request.json()
    updates = body.dict(exclude_none=True)
    # Always include these fields if the client sent them (even as null — means "clear")
    for field in ("logo_url", "primary_color", "description", "contact_email", "contact_phone", "website"):
        if field in raw_body:
            updates[field] = raw_body[field] if raw_body[field] else None
    if not updates: raise HTTPException(400, "Nothing to update")
    sb = get_sb()
    await run(lambda: sb.table("companies").update(updates).eq("id", cid).execute())
    return await get_company(cid, _)

@app.delete("/api/companies/{cid}")
async def delete_company(cid: str, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("companies").delete().eq("id", cid).execute())
    return {"message": "Company deleted"}

@app.post("/api/companies/{cid}/verify")
async def verify_company(cid: str, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("companies").update({
        "status": "active", "verified_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", cid).execute())
    return {"status": "active"}

@app.post("/api/companies/{cid}/suspend")
async def suspend_company(cid: str, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("companies").update({"status": "suspended"}).eq("id", cid).execute())
    return {"status": "suspended"}

@app.post("/api/companies/{cid}/topup")
async def topup_company(cid: str, body: TopUpReq, _=Depends(require_admin)):
    sb = get_sb()
    res = await run(lambda: sb.table("companies").select("credit_balance").eq("id", cid).single().execute())
    if not res.data: raise HTTPException(404, "Company not found")
    new_bal = float(res.data["credit_balance"] or 0) + body.amount
    await run(lambda: sb.table("companies").update({"credit_balance": new_bal}).eq("id", cid).execute())
    return {"credit_balance": new_bal}

# ═══════════════════════════════════════════════════════════════════════════════
# SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/settings")
async def get_settings(_=Depends(require_admin)):
    sb = get_sb()
    rows = await run(lambda: sb.table("platform_config").select("key,value,description,updated_at").order("key").execute())
    return {r["key"]: {"value": r["value"], "description": r.get("description"), "updated_at": str(r.get("updated_at"))} for r in (rows.data or [])}

@app.put("/api/settings")
async def update_settings(body: SettingUpdate, _=Depends(require_admin)):
    sb = get_sb()
    now = datetime.now(timezone.utc).isoformat()
    rows = [{"key": k, "value": json.dumps(v) if not isinstance(v, str) else json.dumps(v), "updated_at": now} for k, v in body.settings.items()]
    for row in rows:
        await run(lambda r=row: sb.table("platform_config").upsert(r, on_conflict="key").execute())
    return {"message": "Settings saved", "updated": list(body.settings.keys())}

# ═══════════════════════════════════════════════════════════════════════════════
# ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/analytics")
async def get_analytics(days: int = 30, _=Depends(require_admin)):
    sb = get_sb()
    try:
        def q_top_earners():   return sb.table("users").select("first_name,last_name,total_earned,games_played,games_won").order("total_earned", desc=True).limit(6).execute()
        def q_company_spend(): return sb.table("companies").select("name,total_spent,credit_balance,status").order("total_spent", desc=True).execute()
        def q_total_users():   return sb.table("users").select("id", count="exact").execute()
        def q_banned():        return sb.table("users").select("id", count="exact").eq("is_banned", True).execute()
        def q_streak():        return sb.table("users").select("id", count="exact").gt("current_streak", 1).execute()
        def q_wd_total():      return sb.table("withdrawals").select("id", count="exact").execute()
        def q_wd_done():       return sb.table("withdrawals").select("id", count="exact").eq("status", "completed").execute()
        # FIX 1: use real commission_etb from deposits
        def q_fee_income():    return sb.table("company_deposits").select("commission_etb").eq("status", "confirmed").execute()

        (r_earners, r_comp, r_users, r_banned,
         r_streak, r_wd_total, r_wd_done, r_fee) = await gather(
            q_top_earners, q_company_spend, q_total_users, q_banned,
            q_streak, q_wd_total, q_wd_done, q_fee_income
        )

        total_users = r_users.count or 0
        banned      = r_banned.count or 0
        streak      = r_streak.count or 0
        wd_total    = r_wd_total.count or 0
        wd_done     = r_wd_done.count or 0
        fee_income  = sum(float(d.get("commission_etb") or 0) for d in (r_fee.data or []))

        return {
            "user_growth": [], "payout_trend": [], "question_accuracy_by_category": [], "level_stats": None,
            "top_earners":   r_earners.data or [],
            "company_spend": r_comp.data or [],
            "health": {
                "ban_rate":                round(banned / total_users * 100, 1) if total_users else 0,
                "retention_rate":          round(streak  / total_users * 100, 1) if total_users else 0,
                "withdrawal_success_rate": round(wd_done  / wd_total   * 100, 1) if wd_total   else 0,
                "platform_fee_income":     fee_income,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Analytics error: %s", e)
        raise HTTPException(500, str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# QR CODES
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/qr/scans")
async def list_qr_scans(_=Depends(require_admin), limit: int = 50, game_id: str = ""):
    """Admin: returns recent scans with full player, QR, and game details."""
    sb = get_sb()
    try:
        def _q():
            q = sb.table("qr_scans").select(
                "id,qr_code_id,qr_token,game_id,user_id,telegram_id,phone_number,"
                "entry_status,block_reason,scanned_at"
            ).order("scanned_at", desc=True).limit(limit)
            if game_id: q = q.eq("game_id", game_id)
            return q.execute()
        res = await run(_q)
        scans = res.data or []
    except Exception as e:
        logger.error("list_qr_scans query failed: %s", e)
        return []

    if not scans:
        return []

    user_ids  = list({s["user_id"]    for s in scans if s.get("user_id")})
    qr_ids    = list({s["qr_code_id"] for s in scans if s.get("qr_code_id")})
    g_ids     = list({s["game_id"]    for s in scans if s.get("game_id")})

    users_map, qr_map, game_map = {}, {}, {}

    try:
        if user_ids:
            ur = await run(lambda: sb.table("users").select(
                "id,first_name,last_name,telegram_username,phone_number"
            ).in_("id", user_ids).execute())
            users_map = {u["id"]: u for u in (ur.data or [])}
    except Exception: pass

    try:
        if qr_ids:
            qrr = await run(lambda: sb.table("qr_codes").select(
                "id,token,label,status,scan_count"
            ).in_("id", qr_ids).execute())
            qr_map = {q["id"]: q for q in (qrr.data or [])}
    except Exception: pass

    try:
        if g_ids:
            gr = await run(lambda: sb.table("games").select(
                "id,title,status"
            ).in_("id", g_ids).execute())
            game_map = {g["id"]: g for g in (gr.data or [])}
    except Exception: pass

    for s in scans:
        s["users"]    = users_map.get(s.get("user_id")) or {}
        s["qr_codes"] = qr_map.get(s.get("qr_code_id")) or {}
        s["games"]    = game_map.get(s.get("game_id")) or {}

    return scans

@app.get("/api/qr")
async def list_qr(_=Depends(require_admin), game_id: str = "", company_id: str = "", status: str = ""):
    sb = get_sb()
    def _q():
        q = sb.table("qr_codes").select(
            "*, games!qr_codes_game_id_fkey(title,game_date,status), companies!qr_codes_company_id_fkey(name)"
        ).order("created_at", desc=True).limit(200)
        if game_id:    q = q.eq("game_id", game_id)
        if company_id: q = q.eq("company_id", company_id)
        if status:     q = q.eq("status", status)
        return q.execute()
    res = await run(_q)
    return res.data or []

# External QR image API — store this URL in DB; never generate PNG locally
QR_IMAGE_API_BASE = "https://api.qrserver.com/v1/create-qr-code/?size=160x160&data="

@app.post("/api/qr")
async def create_qr(body: QRCreateReq, _=Depends(require_admin)):
    sb = get_sb()
    admin = await run(lambda: sb.table("users").select("id").eq("role","admin").limit(1).execute())
    created_by = (admin.data or [{}])[0].get("id")
    token = "SHQ_" + _secrets.token_hex(16).upper()
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=body.expiry_hours)).isoformat() if body.expiry_hours > 0 else None
    base_url = body.base_url.rstrip("?")
    qr_url   = f"{base_url}?startapp={token}"
    # Store exact external API URL for QR image (no local generation)
    qr_image_url = QR_IMAGE_API_BASE + quote(qr_url, safe="")
    payload  = {
        "token": token, "game_id": body.game_id, "company_id": body.company_id or None,
        "created_by": created_by, "label": body.label, "qr_url": qr_url,
        "qr_image_url": qr_image_url,
        "base_url": body.base_url, "max_scans": body.max_scans, "expires_at": expires_at,
        "status": "active", "scan_count": 0
    }
    res = await run(lambda: sb.table("qr_codes").insert(payload).execute())
    qr_row = (res.data or [{}])[0]
    # Notification center: notify users when a QR code is generated (fire-and-forget)
    try:
        game_id = qr_row.get("game_id")
        company_id = qr_row.get("company_id")
        game_data = None
        company_data = None
        if game_id:
            g_res = await run(lambda: sb.table("games").select(
                "id,title,game_date,status,prize_pool_etb,total_players,ends_at,company_id"
            ).eq("id", game_id).single().execute())
            game_data = g_res.data
        if company_id:
            c_res = await run(lambda: sb.table("companies").select("id,name").eq("id", company_id).single().execute())
            company_data = c_res.data
        if qr_row and game_data:
            asyncio.create_task(trigger_broadcast_qr(qr_row, game_data, company_data))
    except Exception as e:
        logger.warning("QR broadcast trigger skipped: %s", e)
    return qr_row

@app.post("/api/qr/delete")
async def delete_qr(body: QRDeleteReq, _=Depends(require_admin)):
    """
    Permanently delete a QR code and all related data.
    Body: {"id": "<qr_codes.uuid>"}
    - Unlinks game_sessions (qr_code_id = NULL)
    - Deletes qr_scans, then qr_codes row. Idempotent.
    """
    qid = (body.id or "").strip()
    if not qid:
        raise HTTPException(400, "Missing QR code id")
    logger.info("delete_qr: qid=%s", qid)
    sb = get_sb()
    try:
        await run(lambda: sb.table("game_sessions").update({"qr_code_id": None}).eq("qr_code_id", qid).execute())
        await run(lambda: sb.table("qr_scans").delete().eq("qr_code_id", qid).execute())
        await run(lambda: sb.table("qr_codes").delete().eq("id", qid).execute())
    except Exception as e:
        logger.warning("delete_qr %s: %s", qid, e)
        raise HTTPException(500, str(e))
    return {"ok": True, "message": "QR code deleted", "id": qid}

@app.post("/api/qr/validate")
async def validate_qr_token(body: QRScanReq):
    """
    Public — mini-app calls to validate a scanned QR before showing Join sheet.

    FIX 2: now also checks whether this user (by user_id OR telegram_id) already
    has an entry in qr_scans for this game, returning already_played=True so the
    mini-app can block re-entry immediately.
    """
    sb = get_sb()
    res = await run(lambda: sb.table("qr_codes").select(
        "*, games!qr_codes_game_id_fkey(id,title,status,game_date,prize_pool_etb,prize_pool_remaining),"
        "companies!qr_codes_company_id_fkey(name)"
    ).eq("token", body.token).limit(1).execute())

    rows = res.data or []
    if not rows:
        return {"ok": False, "reason": "Invalid QR code"}

    qr = rows[0]

    if qr["status"] != "active":
        return {"ok": False, "reason": f"QR code is {qr['status']}"}

    if qr["expires_at"]:
        exp = datetime.fromisoformat(qr["expires_at"].replace("Z", "+00:00"))
        if exp < datetime.now(timezone.utc):
            await run(lambda: sb.table("qr_codes").update({"status": "expired"}).eq("id", qr["id"]).execute())
            return {"ok": False, "reason": "QR code has expired"}

    if qr["max_scans"] > 0 and qr["scan_count"] >= qr["max_scans"]:
        return {"ok": False, "reason": "QR code scan limit reached"}

    game = qr.get("games") or {}
    if game.get("status") not in ("active", "scheduled"):
        return {"ok": False, "reason": f"Game is {game.get('status', 'unavailable')}"}

    game_id = game.get("id")

    # One scan per QR code (new QR = new play): check only this qr_code_id
    qr_code_id = qr["id"]
    if body.user_id or body.telegram_id:
        def _check_scan():
            q = sb.table("qr_scans").select("id,entry_status").eq("qr_code_id", qr_code_id)
            if body.user_id:
                q = q.eq("user_id", body.user_id)
            elif body.telegram_id:
                q = q.eq("telegram_id", body.telegram_id)
            return q.limit(1).execute()

        scan_res = await run(_check_scan)
        if scan_res.data:
            return {
                "ok": False,
                "reason": "You already used this QR code",
                "already_played": True
            }

    return {
        "ok": True,
        "qr_code_id":    qr["id"],
        "token":         qr["token"],
        "game_id":       game_id,
        "game_title":    game.get("title"),
        "game_date":     game.get("game_date"),
        "prize_pool_etb": game.get("prize_pool_etb"),
        "prize_pool_remaining": game.get("prize_pool_remaining"),
        "company":       (qr.get("companies") or {}).get("name"),
        "label":         qr.get("label"),
    }

@app.get("/api/qr/{qid}/image")
async def get_qr_image(qid: str):
    """Redirect to stored QR image URL from api.qrserver.com (no local generation)."""
    sb = get_sb()
    res = await run(lambda: sb.table("qr_codes").select("qr_image_url,qr_url").eq("id", qid).limit(1).execute())
    rows = res.data or []
    if not rows:
        raise HTTPException(404, "QR code not found")
    row = rows[0]
    image_url = row.get("qr_image_url")
    if not image_url:
        # Backfill for rows created before qr_image_url existed
        image_url = QR_IMAGE_API_BASE + quote((row.get("qr_url") or ""), safe="")
    return RedirectResponse(url=image_url, status_code=302)

@app.post("/api/qr/scan")
async def record_qr_scan(body: QRScanReq):
    """
    Public — records a scan after user confirms Join.

    FIX 5: SELECT-before-INSERT instead of catching error string.
    Checks by user_id AND telegram_id to block any re-entry attempt.
    """
    sb = get_sb()

    qr_res = await run(lambda: sb.table("qr_codes").select("id,game_id")
        .eq("token", body.token).limit(1).execute())
    qr_rows = qr_res.data or []
    if not qr_rows:
        raise HTTPException(404, "QR code not found")
    qr = qr_rows[0]
    game_id = qr["game_id"]

    # One scan per QR code: check duplicate by this qr_code_id only (new QR = new play)
    qr_code_id = qr["id"]
    if body.user_id or body.telegram_id:
        def _check():
            q = sb.table("qr_scans").select("id").eq("qr_code_id", qr_code_id)
            if body.user_id:    q = q.eq("user_id", body.user_id)
            elif body.telegram_id: q = q.eq("telegram_id", body.telegram_id)
            return q.limit(1).execute()
        dup = await run(_check)
        if dup.data:
            return {"entry_status": "already_scanned", "message": "Already used this QR code"}

    scan = {
        "qr_code_id":   qr["id"],
        "qr_token":     body.token,
        "game_id":      game_id,
        "user_id":      body.user_id or None,
        "telegram_id":  body.telegram_id,
        "phone_number": body.phone_number,
        "entry_status": "entered"
    }
    try:
        res = await run(lambda: sb.table("qr_scans").insert(scan).execute())
        return (res.data or [{}])[0]
    except Exception as e:
        # Fallback: DB unique constraint still catches edge-case race conditions
        err_str = str(e).lower()
        if "unique" in err_str or "duplicate" in err_str or "qr_scans" in err_str:
            return {"entry_status": "already_scanned", "message": "Already used this QR code"}
        raise HTTPException(400, str(e))

@app.post("/api/qr/{qid}/revoke")
async def revoke_qr(qid: str, _=Depends(require_admin)):
    """Revoke a QR code (stops scanning; keeps record). Idempotent."""
    sb = get_sb()
    await run(lambda: sb.table("qr_codes").update({"status": "revoked"}).eq("id", qid).execute())
    return {"ok": True, "status": "revoked", "id": qid}


@app.post("/api/admin/broadcast-qr/{qr_id}")
async def admin_broadcast_qr(qr_id: str, _=Depends(require_admin)):
    """
    Manually trigger notification-center broadcast for a QR code.
    Sends "New QR code ready" to all users with notifications_enabled.
    """
    sb = get_sb()
    qr_res = await run(lambda: sb.table("qr_codes").select("*").eq("id", qr_id).limit(1).execute())
    if not qr_res.data:
        raise HTTPException(404, "QR code not found")
    qr = qr_res.data[0]
    game_id = qr.get("game_id")
    company_id = qr.get("company_id")
    game_data = None
    company_data = None
    if game_id:
        g_res = await run(lambda: sb.table("games").select(
            "id,title,game_date,status,prize_pool_etb,total_players,ends_at,company_id"
        ).eq("id", game_id).single().execute())
        game_data = g_res.data
    if company_id:
        c_res = await run(lambda: sb.table("companies").select("id,name").eq("id", company_id).single().execute())
        company_data = c_res.data
    if not game_data:
        raise HTTPException(400, "QR code has no game")
    try:
        from bot import get_bot_app, broadcast_new_qr
        app = get_bot_app()
        if app and app.bot:
            stats = await broadcast_new_qr(qr, game_data, company_data)
            return {"message": "QR broadcast sent", "qr_id": qr_id, "stats": stats}
        return {"message": "QR broadcast requested", "qr_id": qr_id, "stats": None, "warning": "Bot not available"}
    except Exception as e:
        logger.error("Admin broadcast QR failed: %s", e)
        raise HTTPException(500, str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# GAME SESSIONS & TRIVIA FLOW (public — mini-app)
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/game/session/start")
async def start_session(body: SessionStartReq):
    """
    Start or resume a game session.

    FIX 3: if the existing session is already completed or inactive, block
    re-entry with already_played=True instead of silently returning old session.

    FIX 8: returns answered_question_ids so the mini-app can skip questions
    the user already answered (they are never shown again).
    Returns game_config (seconds_per_question, max_wrong_answers) from admin settings.
    """
    sb = get_sb()
    game_config = await _get_game_config(sb)

    # Check existing session (prefer active one — new QR can create new session)
    existing = await run(lambda: sb.table("game_sessions")
        .select("*").eq("user_id", body.user_id).eq("game_id", body.game_id)
        .order("is_active", desc=True).order("started_at", desc=True).limit(1).execute())

    if existing.data:
        sess = existing.data[0]

        # SECURITY: Once game is over (any path), never allow re-entry
        if sess.get("is_completed"):
            return {"session": sess, "cooldown": False, "already_played": True,
                    "reason": "You already finished this game", "game_config": game_config}

        if sess.get("is_active") is False:
            return {"session": sess, "cooldown": False, "already_played": True,
                    "reason": "Game over — you cannot re-enter", "game_config": game_config}

        max_wrong = game_config.get("max_wrong_answers", 3)
        if sess.get("wrong_count", 0) >= max_wrong:
            return {"session": sess, "cooldown": False, "already_played": True,
                    "reason": "Game over — %d wrong answers" % max_wrong, "game_config": game_config}

        if sess.get("cooldown_until"):
            cd = datetime.fromisoformat(sess["cooldown_until"].replace("Z", "+00:00"))
            if cd > datetime.now(timezone.utc):
                return {"session": sess, "cooldown": True,
                        "cooldown_until": sess["cooldown_until"],
                        "already_played": True, "reason": "Game over — %d wrong answers" % max_wrong, "game_config": game_config}
            # Cooldown expired = game was over (3 wrong), still block re-entry
            return {"session": sess, "cooldown": False, "already_played": True,
                    "reason": "Game over — you cannot re-enter", "game_config": game_config}

        # FIX 8: return already-answered question IDs so mini-app skips them
        answered_res = await run(lambda: sb.table("round_answers")
            .select("question_id").eq("session_id", sess["id"]).execute())
        answered_ids = [r["question_id"] for r in (answered_res.data or [])]

        return {"session": sess, "cooldown": False, "already_played": False,
                "answered_question_ids": answered_ids, "game_config": game_config}

    # Resolve qr_code_id (only when joining via QR)
    qr_code_id = None
    if body.qr_token:
        qr_r = await run(lambda: sb.table("qr_codes").select("id")
            .eq("token", body.qr_token).single().execute())
        if qr_r.data: qr_code_id = qr_r.data["id"]
    else:
        # Attend without QR: must have scanned this game once (qr_scans record)
        scan_check = sb.table("qr_scans").select("id").eq("game_id", body.game_id)
        if body.user_id:
            scan_check = scan_check.eq("user_id", body.user_id)
        elif body.telegram_id is not None:
            scan_check = scan_check.eq("telegram_id", body.telegram_id)
        else:
            raise HTTPException(400, "user_id or telegram_id required to attend")
        scan_res = await run(lambda: scan_check.limit(1).execute())
        if not scan_res.data:
            return {"session": None, "already_played": True, "reason": "Scan the game QR once first to join", "game_config": game_config}

    # Get game config
    game_r = await run(lambda: sb.table("games")
        .select("prize_pool_remaining,player_cap_pct").eq("id", body.game_id).single().execute())
    game = game_r.data or {}
    remaining  = float(game.get("prize_pool_remaining") or 0)
    cap_pct    = float(game.get("player_cap_pct") or 30)
    player_cap = round(remaining * cap_pct / 100, 2)

    sess_payload = {
        "game_id": body.game_id, "user_id": body.user_id,
        "qr_code_id": qr_code_id, "player_cap_etb": player_cap,
        "current_question": 1, "questions_answered": 0,
        "wrong_count": 0, "is_active": True
    }
    res = await run(lambda: sb.table("game_sessions").insert(sess_payload).execute())
    return {"session": (res.data or [{}])[0], "cooldown": False,
            "already_played": False, "answered_question_ids": [], "game_config": game_config}

@app.get("/api/game/{game_id}/questions")
async def get_game_questions_public(game_id: str, session_id: str = ""):
    """
    Public — returns ordered questions for a game WITHOUT is_correct exposed.

    FIX 4: is_correct is STRIPPED from every option before returning.
    FIX 8: if session_id is provided, questions already answered in that session
           are filtered out (never shown again).

    The mini-app passes session_id as a query param so only unseen questions
    are returned on reconnect.
    """
    sb = get_sb()

    # Get answered question IDs for this session (if resuming)
    answered_ids: set = set()
    if session_id:
        ans_res = await run(lambda: sb.table("round_answers")
            .select("question_id").eq("session_id", session_id).execute())
        answered_ids = {r["question_id"] for r in (ans_res.data or [])}

    gq_res = await run(lambda: sb.table("game_questions").select(
        "sort_order, questions(id, icon, question_text, category, explanation, status)"
    ).eq("game_id", game_id).order("sort_order").execute())

    rows = gq_res.data or []
    q_ids = [r["questions"]["id"] for r in rows if r.get("questions") and r["questions"].get("status") == "approved"]

    if not q_ids:
        return []

    opts_res = await run(lambda: sb.table("answer_options")
        .select("id,question_id,option_letter,option_text,sort_order")
        # FIX 4: deliberately NOT selecting is_correct
        .in_("question_id", q_ids).order("sort_order").execute())

    opts_map: Dict[str, list] = {}
    for o in (opts_res.data or []):
        opts_map.setdefault(o["question_id"], []).append(o)

    questions = []
    for r in rows:
        q = r.get("questions")
        if not q or q.get("status") != "approved":
            continue
        # FIX 8: skip already answered questions
        if q["id"] in answered_ids:
            continue
        q["sort_order"] = r["sort_order"]
        q["options"]    = opts_map.get(q["id"], [])
        questions.append(q)

    return questions

@app.post("/api/game/answer")
async def submit_answer(body: AnswerReq):
    """
    Record a player's answer.
    Server verifies is_correct from DB — never trusts client.
    Returns is_correct, updated session state, and wrong_count.
    """
    sb = get_sb()

    is_correct  = False
    status_val  = "timeout"

    if body.selected_option_id:
        # Server-side truth check — client never knows is_correct in advance
        try:
            opt_res = await run(lambda: sb.table("answer_options")
                .select("is_correct").eq("id", str(body.selected_option_id).strip()).limit(1).execute())
            row = (opt_res.data or [{}])[0] if opt_res.data else {}
            is_correct = row.get("is_correct") is True
            status_val = "correct" if is_correct else "wrong"
        except Exception as e:
            logger.warning("submit_answer option lookup: %s", e)
            is_correct = False
            status_val = "wrong"

    ans_payload = {
        "session_id":        body.session_id,
        "user_id":           body.user_id,
        "game_id":           body.game_id,
        "question_id":       body.question_id,
        "selected_option_id": body.selected_option_id,
        "question_number":   body.question_number,
        "is_correct":        is_correct,
        "status":            status_val,
        "time_taken_ms":     body.time_taken_ms,
    }
    await run(lambda: sb.table("round_answers").insert(ans_payload).execute())

    # DB trigger (trg_on_round_answer) updates wrong_count and cooldown automatically
    # Fetch updated session to return to client
    sess_res = await run(lambda: sb.table("game_sessions")
        .select("*").eq("id", body.session_id).single().execute())
    sess = sess_res.data or {}

    # Also return the correct option_id so mini-app can highlight it
    correct_opt_res = await run(lambda: sb.table("answer_options")
        .select("id").eq("question_id", body.question_id).eq("is_correct", True).limit(1).execute())
    correct_option_id = ((correct_opt_res.data or [{}])[0]).get("id")

    return {
        "is_correct":        is_correct,
        "status":            status_val,
        "correct_option_id": correct_option_id,
        "session":           sess,
        "wrong_count":       sess.get("wrong_count", 0),
        "cooldown_until":    sess.get("cooldown_until"),
    }

@app.post("/api/game/spin")
async def record_spin(body: SpinReq):
    """
    Record a spin result. Credits user balance via DB trigger (trg_on_spin_insert).

    FIX 6: enforces player_cap_etb — rejects spin if user's session earnings
    would exceed their cap for this game.
    """
    sb = get_sb()

    # FIX 6: load current session to check player cap
    sess_res = await run(lambda: sb.table("game_sessions")
        .select("total_earned,player_cap_etb").eq("id", body.session_id).single().execute())
    sess_data = sess_res.data or {}
    total_so_far = float(sess_data.get("total_earned") or 0)
    cap          = float(sess_data.get("player_cap_etb") or 0)

    # If cap is set and this spin would exceed it, clamp the amount
    amount = body.amount_etb
    if cap > 0 and (total_so_far + amount) > cap:
        amount = max(0, cap - total_so_far)
        if amount <= 0:
            # Cap already reached — return without crediting
            user_res = await run(lambda: sb.table("users")
                .select("balance,total_earned").eq("id", body.user_id).single().execute())
            return {"spin": None, "user": user_res.data, "cap_reached": True, "amount_credited": 0}

    spin_payload = {
        "session_id":     body.session_id,
        "user_id":        body.user_id,
        "game_id":        body.game_id,
        "question_number": body.question_number,
        "segment_label":  body.segment_label,
        "amount_etb":     amount,  # clamped amount; w-status defaults to 'active' (migration 012)
    }
    res = await run(lambda: sb.table("spin_results").insert(spin_payload).execute())

    # Update session progress counters
    await run(lambda: sb.table("game_sessions").update({
        "current_question":   body.question_number + 1,
        "questions_answered": body.question_number,
    }).eq("id", body.session_id).execute())

    # Get updated user balance (trigger already credited it)
    user_res = await run(lambda: sb.table("users")
        .select("balance,total_earned").eq("id", body.user_id).single().execute())

    return {
        "spin":           (res.data or [{}])[0],
        "user":           user_res.data,
        "cap_reached":    False,
        "amount_credited": amount,
    }

@app.post("/api/game/session/{sid}/end")
async def end_session(sid: str, user_id: str):
    """Mark session as ended (game completed or game over). Only session owner can end."""
    sb = get_sb()
    res = await run(lambda: sb.table("game_sessions").update({
        "is_active":    False,
        "is_completed": True,
        "ended_at":     datetime.now(timezone.utc).isoformat()
    }).eq("id", sid).eq("user_id", user_id).execute())
    if not res.data:
        raise HTTPException(404, "Session not found or not yours")
    sess = await run(lambda: sb.table("game_sessions").select("*").eq("id", sid).single().execute())
    return sess.data

# ═══════════════════════════════════════════════════════════════════════════════
# COMPANY DEPOSITS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/deposits")
async def list_deposits(_=Depends(require_admin), status: str = "", company_id: str = "", page: int = 1, per_page: int = 20, search: str = ""):
    sb = get_sb()

    def _q():
        q = sb.table("company_deposits").select("*", count="exact").order("created_at", desc=True)
        if status:
            q = q.eq("status", status)
        if company_id:
            q = q.eq("company_id", company_id)
        if search and search.strip():
            s = search.strip().replace("%", "")[:80]
            q = q.or_(f"ref_number.ilike.%{s}%,notes.ilike.%{s}%")
        offset = (page - 1) * per_page
        q = q.range(offset, offset + per_page - 1)
        return q.execute()

    res = await run(_q)
    rows = res.data or []
    total = getattr(res, "count", len(rows))

    # Resolve company names separately (no embed — avoids FK/schema issues)
    if rows:
        cids = list({r.get("company_id") for r in rows if r.get("company_id")})
        try:
            co_res = await run(
                lambda: sb.table("companies").select("id,name").in_("id", cids).execute()
            )
            co_map = {c["id"]: c.get("name") or "—" for c in (co_res.data or [])}
        except Exception:
            co_map = {}
        for r in rows:
            r["company_name"] = co_map.get(r.get("company_id")) or "—"

    return {"data": rows, "total": total, "page": page, "per_page": per_page}

@app.post("/api/deposits/{did}/approve")
async def approve_deposit(did: str, body: DepositApproveReq, _=Depends(require_admin)):
    sb = get_sb()
    admin = await run(lambda: sb.table("users").select("id").eq("role","admin").limit(1).execute())
    confirmed_by = (admin.data or [{}])[0].get("id")
    # Trigger on_deposit_confirmed handles commission_etb and prize_pool_etb calculation
    await run(lambda: sb.table("company_deposits").update({
        "status": "confirmed", "notes": body.notes, "confirmed_by": confirmed_by
    }).eq("id", did).eq("status", "pending").execute())
    return {"status": "confirmed"}

@app.post("/api/deposits/{did}/reject")
async def reject_deposit(did: str, body: DepositRejectReq, _=Depends(require_admin)):
    sb = get_sb()
    await run(lambda: sb.table("company_deposits").update({
        "status": "rejected", "rejected_reason": body.reason
    }).eq("id", did).eq("status", "pending").execute())
    return {"status": "rejected"}

# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def startup():
    logger.info("PORT=%s API_BASE_URL=%s APP_ENV=%s (VPS=%s)", PORT, API_BASE_URL, APP_ENV, IS_VPS)
    try:
        get_sb()
        logger.info("✅ Supabase singleton client ready")
    except Exception as e:
        logger.warning("⚠️  Supabase init warning: %s", e)
