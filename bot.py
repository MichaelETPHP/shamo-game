"""
SHAMO Telegram Bot
==================
- /start  → asks user to share phone number
- Contact → registers user via API, sends Mini App launch button
- /play   → sends Mini App button directly
- /help   → shows help

Runs alongside api.py (invoked from api.py startup event).
Can also run standalone: python bot.py
"""

import asyncio
import logging
import os
import socket
import sys
import hmac
import hashlib
from urllib.parse import parse_qsl

import httpx
from telegram.error import Conflict, Forbidden, BadRequest, RetryAfter

from telegram import (
    Update,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Auto-delete delays (seconds) — bot can only delete its own messages, within 48 hours (Telegram limit)
AUTO_DELETE_DELAY = 1 * 60  # 1 minute for all bot messages (clean chat)

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Single-instance lock (socket on localhost) ──────────────
_LOCK_PORT = 47201  # arbitrary port; change if it conflicts
_lock_socket: socket.socket | None = None

def acquire_instance_lock() -> bool:
    """Returns True if this is the only running instance, False otherwise."""
    global _lock_socket
    try:
        _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        _lock_socket.bind(("127.0.0.1", _LOCK_PORT))
        _lock_socket.listen(1)
        return True
    except OSError:
        return False

# ─── .env loader ────────────────────────────────────────────
def _project_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))

def load_dotenv(path: str = ".env") -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, value)
    except FileNotFoundError:
        pass

# Load .env from project root (same dir as bot.py) so it works regardless of cwd
load_dotenv(os.path.join(_project_dir(), ".env"))

# ─── Config (all from .env — see .env.example.local) ─────────────────────────
BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
SHAMO_WEBAPP_URL = (os.getenv("SHAMO_WEBAPP_URL") or "").strip()
API_BASE_URL     = (os.getenv("API_BASE_URL") or "").rstrip("/")


# ─── Telegram initData validator ─────────────────────────────
def validate_init_data(init_data: str, bot_token: str) -> bool:
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return False
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(received_hash, expected_hash)
    except Exception as e:
        logger.error("init_data validation error: %s", e)
        return False


# ─── API helper — register/update player via our FastAPI ─────
async def get_profile_photo_url(bot, user_id: int) -> str | None:
    """Get user's profile photo URL (largest size). Required for secure registration."""
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if not photos or not photos.photos:
            return None
        # Get largest photo (last in the first row)
        largest = photos.photos[0][-1]
        file = await bot.get_file(largest.file_id)
        # Construct full URL: https://api.telegram.org/file/bot<token>/<path>
        return file.file_path and f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    except Exception as e:
        logger.warning("Could not get profile photo for %s: %s", user_id, e)
    return None


async def register_player_via_api(
    bot,
    telegram_id: int,
    first_name: str,
    last_name: str | None,
    username: str | None,
    language_code: str | None,
    phone_number: str,
    photo_url: str | None = None,
) -> dict | None:
    """Register or update player via /api/player/login (PostgreSQL DB). Sends telegram_id, phone, profile image."""
    if not photo_url and bot:
        photo_url = await get_profile_photo_url(bot, telegram_id)
    payload = {
        "telegram_id": telegram_id,
        "first_name": first_name,
        "last_name": last_name,
        "telegram_username": username,
        "language_code": language_code or "en",
        "phone_number": phone_number,
        "photo_url": photo_url,
    }
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30, http2=False) as client:
                res = await client.post(f"{API_BASE_URL}/api/player/login", json=payload)
                if res.status_code == 200:
                    logger.info("Player registered: tg_id=%s phone=*** photo=%s", telegram_id, "yes" if photo_url else "no")
                    return res.json()
                else:
                    logger.warning("API login failed: %s %s", res.status_code, res.text)
        except Exception as e:
            logger.warning("register_player attempt %d/3: %s", attempt + 1, e)
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
            else:
                logger.error("API unreachable after 3 attempts. Check API_BASE_URL=%s", API_BASE_URL)
    return None


# ─── Keyboards ───────────────────────────────────────────────
def _contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton(text="📱 Share my phone number", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def _schedule_delete_message(bot, chat_id: int, message_id: int, delay_seconds: int) -> None:
    """
    Schedule deletion of the bot's own message after delay. Force-delete with retries.
    Telegram allows delete only within 48 hours. Runs in background; does not block.
    """
    if delay_seconds <= 0:
        return
    chat_id = int(chat_id)
    message_id = int(message_id)

    async def _do_delete() -> None:
        try:
            await asyncio.sleep(delay_seconds)
        except asyncio.CancelledError:
            return
        # Force delete with retries (Telegram can be briefly inconsistent)
        for attempt in range(3):
            try:
                await bot.delete_message(chat_id=chat_id, message_id=message_id)
                logger.info("Auto-delete: removed message %s in chat %s", message_id, chat_id)
                return
            except Forbidden:
                return  # no permission or chat left
            except BadRequest as e:
                err = str(e).lower()
                if "message to delete not found" in err or "message can't be deleted" in err:
                    return
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                logger.warning("Auto-delete failed chat=%s msg=%s: %s", chat_id, message_id, e)
                return
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                logger.warning("Auto-delete failed chat=%s msg=%s: %s", chat_id, message_id, e)
                return

    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_do_delete())
        # Keep reference so task is not lost (optional but helps in some environments)
        if not hasattr(_schedule_delete_message, "_tasks"):
            _schedule_delete_message._tasks = set()
        _schedule_delete_message._tasks.discard(task)
        _schedule_delete_message._tasks.add(task)
        task.add_done_callback(lambda t: _schedule_delete_message._tasks.discard(t))
    except RuntimeError:
        logger.warning("Auto-delete: no running event loop, cannot schedule delete")


def _webapp_keyboard(payload: str = None) -> InlineKeyboardMarkup:
    # If payload provided, append it to the WebApp URL (deep-linking)
    # Telegram sends /start payload as startapp, which opens the WebApp with tgWebAppStartParam
    url = SHAMO_WEBAPP_URL
    if payload:
        # Example: shamo-app.html?qr=SHQ_1234
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}qr={payload}"
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(text="🎮 Play SHAMO", web_app=WebAppInfo(url=url))
    ]])


# ─── Handlers ────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start — welcome + phone-share button. Supports deep linking params."""
    user = update.effective_user
    args = context.args
    payload = args[0] if args else None
    
    # If payload provided, we store it in user_data so contact_handler can access it
    if payload:
        context.user_data["startapp_payload"] = payload

    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            f"👋 Welcome to *SHAMO*, {user.first_name}! ሻሞ 🎯\n\n"
            "Answer 3 Ethiopian culture questions, spin the wheel and win prizes!\n\n"
            "📱 Please share your phone number to register and start playing."
        ),
        reply_markup=_contact_keyboard(),
        parse_mode="Markdown",
    )
    _schedule_delete_message(
        context.bot, update.effective_chat.id, msg.message_id, AUTO_DELETE_DELAY
    )


async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Receives phone contact (from /start or Mini App) → save phone to DB → register profile → Mini App button."""
    if not update.message or not update.message.contact:
        logger.warning("contact_handler: no message or contact")
        return

    contact  = update.message.contact
    user     = update.effective_user
    chat_id  = update.effective_chat.id

    raw = (contact.phone_number or "").strip()
    digits = "".join(c for c in raw if c.isdigit())
    if not digits or len(digits) < 9:
        logger.warning("contact_handler: empty or invalid phone")
        return
    if digits.startswith("0"):
        digits = "251" + digits[1:]
    elif not digits.startswith("251") and len(digits) == 9:
        digits = "251" + digits
    phone = "+" + digits

    # Format phone for display (mask middle)
    phone_display = phone[:6] + "***" + phone[-2:] if len(phone) >= 8 else phone[:4] + "***"
    logger.info("Contact received: tg_id=%s @%s phone=%s", user.id, user.username or "—", phone_display)

    # 1) Save phone to DB immediately — Mini App is polling for this (retry on connection failure)
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30, http2=False) as client:
                r = await client.post(
                    f"{API_BASE_URL}/api/player/phone-by-telegram",
                    json={
                        "telegram_id": user.id,
                        "phone_number": phone,
                        "first_name": user.first_name or "Player",
                    },
                )
                if r.status_code == 200:
                    logger.info("✅ Phone saved to DB: tg_id=%s", user.id)
                    break
                else:
                    logger.warning("⚠️ phone-by-telegram failed: %s — %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("phone-by-telegram attempt %d/3: %s", attempt + 1, e)
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
            else:
                logger.error("❌ phone-by-telegram unreachable after 3 attempts. Check API_BASE_URL=%s", API_BASE_URL)

    # 2) Full register/update with profile and photo
    await register_player_via_api(
        bot=context.bot,
        telegram_id=user.id,
        first_name=user.first_name or "",
        last_name=user.last_name,
        username=user.username,
        language_code=user.language_code,
        phone_number=phone,
    )

    # Confirm: show shared info (username + phone) — Telegram provides this when user shares
    handle = f"@{user.username}" if user.username else user.first_name or "User"
    msg1 = await context.bot.send_message(
        chat_id=chat_id,
        text=f"✅ *Shared & saved*\n\n{handle} · {phone}\n\nYou can continue in the app.",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="Markdown",
    )
    _schedule_delete_message(
        context.bot, chat_id, msg1.message_id, AUTO_DELETE_DELAY
    )

    # Retrieve any deep-linking payload passed in /start
    payload = context.user_data.pop("startapp_payload", None)

    # Send Mini App button
    msg2 = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🏆 *SHAMO Prize Game* ሻሞ\n\n"
            "• Answer 3 Ethiopian culture questions ❓\n"
            "• Spin the wheel to win real money 🎡\n"
            "• Prizes paid instantly via Telebirr 💳\n\n"
            "_All answers must be correct to unlock your spin!_"
        ),
        reply_markup=_webapp_keyboard(payload),
        parse_mode="Markdown",
    )
    _schedule_delete_message(
        context.bot, chat_id, msg2.message_id, AUTO_DELETE_DELAY
    )


async def play_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/play — quick shortcut to open the Mini App."""
    args = context.args
    payload = args[0] if args else None

    msg = await update.message.reply_text(
        text="🎮 Open SHAMO now 👇",
        reply_markup=_webapp_keyboard(payload),
    )
    _schedule_delete_message(
        context.bot, update.effective_chat.id, msg.message_id, AUTO_DELETE_DELAY
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/help — list available commands."""
    msg = await update.message.reply_text(
        "📋 *SHAMO Commands*\n\n"
        "/start — register with your phone & get game link\n"
        "/play  — open the SHAMO Mini App directly\n"
        "/help  — show this message\n\n",
        parse_mode="Markdown",
    )
    _schedule_delete_message(
        context.bot, update.effective_chat.id, msg.message_id, AUTO_DELETE_DELAY
    )


async def unknown_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await update.message.reply_text("❓ Unknown command. Try /help")
    _schedule_delete_message(
        context.bot, update.effective_chat.id, msg.message_id, AUTO_DELETE_DELAY
    )


# ─── Error handler ───────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Conflict):
        logger.critical(
            "Conflict error: another bot instance is running. Shutting this instance down."
        )
        # Stop polling gracefully so the process exits cleanly
        asyncio.get_event_loop().call_soon_threadsafe(
            context.application.stop
        )
        return
    logger.error("Bot error: %s", context.error, exc_info=context.error)


# ─── Global app instance (for API to trigger broadcasts) ───────
_app: Application | None = None

def get_bot_app() -> Application | None:
    """Return the running bot Application instance, or None if not set."""
    return _app

def set_bot_app(app: Application) -> None:
    """Store the bot Application instance globally."""
    global _app
    _app = app


# ─── Broadcast: notify all users of new game ───────────────────
def _format_starts_at(starts_at_raw) -> str:
    """Format starts_at as 'Today at 3:00 PM' or 'Feb 28 at 3:00 PM'."""
    from datetime import datetime, timezone
    if not starts_at_raw:
        return "Soon"
    try:
        dt = datetime.fromisoformat(str(starts_at_raw).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        time_str = dt.strftime("%I:%M %p").lstrip("0")  # "3:00 PM"
        if dt.date() == now.date():
            return f"Today at {time_str}"
        return f"{dt.strftime('%b %d')} at {time_str}"
    except Exception:
        return str(starts_at_raw)[:19] if starts_at_raw else "Soon"


def _format_expires_at(expires_at_raw) -> str:
    """Format expiry as 'Today at 9:00 PM' or 'Feb 28 at 9:00 PM'. Same as _format_starts_at."""
    return _format_starts_at(expires_at_raw)


async def broadcast_new_game(game: dict) -> dict:
    """
    Send new game notification to ALL users with telegram_id.
    Uses direct PostgreSQL via db module.
    """
    import db as _db

    sent = 0
    failed = 0
    blocked = 0
    total = 0

    try:
        users = await asyncio.get_event_loop().run_in_executor(None, lambda: _db.execute(
            "SELECT id, telegram_id FROM users WHERE telegram_id IS NOT NULL AND is_active = TRUE AND is_banned = FALSE",
            fetch="all", commit=False
        ))
        users = [u for u in (users or []) if u.get("telegram_id") is not None]
        total = len(users)

        if total == 0:
            logger.info("Broadcast: no users to notify")
            return {"sent": 0, "failed": 0, "blocked": 0, "total": 0}

        app = get_bot_app()
        if not app or not app.bot:
            logger.warning("Broadcast: bot not available")
            return {"sent": 0, "failed": 0, "blocked": 0, "total": total}

        bot = app.bot
        company_name = (game.get("companies") or {}).get("name") or game.get("company_name") or "SHAMO"
        prize_pool_etb = game.get("prize_pool_etb") or 0
        starts_at = _format_starts_at(game.get("starts_at"))

        msg_text = (
            "🎮 *New Game Live!*\n\n"
            f"🏢 {company_name}\n"
            f"💰 Prize Pool: {prize_pool_etb} ETB\n"
            f"⏰ Starts: {starts_at}\n\n"
            "Answer questions · Spin the wheel · Win real Birr!"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(text="🎯 Play Now", web_app=WebAppInfo(url=SHAMO_WEBAPP_URL))
        ]])
        loop = asyncio.get_event_loop()

        for i, user in enumerate(users):
            tg_id = user.get("telegram_id")
            if tg_id is None:
                continue
            try:
                await bot.send_message(
                    chat_id=tg_id, text=msg_text,
                    reply_markup=keyboard, parse_mode="Markdown",
                )
                sent += 1
            except Forbidden:
                blocked += 1
            except BadRequest:
                failed += 1
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    await bot.send_message(chat_id=tg_id, text=msg_text,
                                           reply_markup=keyboard, parse_mode="Markdown")
                    sent += 1
                except Exception:
                    failed += 1
            except Exception as e:
                logger.warning("Broadcast: failed for tg_id=%s: %s", tg_id, e)
                failed += 1
            await asyncio.sleep(0.05)
            if (i + 1) % 50 == 0:
                logger.info("Broadcast progress: %d/%d", i + 1, total)

        logger.info("Broadcast done: sent=%d failed=%d blocked=%d", sent, failed, blocked)
        return {"sent": sent, "failed": failed, "blocked": blocked, "total": total}
    except Exception as e:
        logger.error("Broadcast fetch failed: %s", e)
        return {"sent": 0, "failed": 0, "blocked": 0, "total": 0}


# ─── Broadcast: notify users when a QR code is generated (notification center) ─
async def broadcast_new_qr(qr: dict, game: dict, company: dict | None = None) -> dict:
    """
    Send "New game live + QR image" to all users with telegram_id.
    Uses direct PostgreSQL via db module.
    """
    import db as _db

    sent = 0
    failed = 0
    blocked = 0
    total = 0

    try:
        users = await asyncio.get_event_loop().run_in_executor(None, lambda: _db.execute(
            "SELECT id, telegram_id FROM users WHERE telegram_id IS NOT NULL AND is_active = TRUE AND is_banned = FALSE",
            fetch="all", commit=False
        ))
        users = [u for u in (users or []) if u.get("telegram_id") is not None]
        total = len(users)

        if total == 0:
            logger.info("Broadcast QR: no users to notify")
            return {"sent": 0, "failed": 0, "blocked": 0, "total": 0}

        app = get_bot_app()
        if not app or not app.bot:
            logger.warning("Broadcast QR: bot not available")
            return {"sent": 0, "failed": 0, "blocked": 0, "total": total}

        bot = app.bot
        game_title    = (game or {}).get("title") or "SHAMO Game"
        company_name  = (company or {}).get("name") if company else None
        prize_pool    = (game or {}).get("prize_pool_etb") or 0
        total_players = (game or {}).get("total_players") or 0
        expires_raw   = (qr or {}).get("expires_at") or (game or {}).get("ends_at")
        expires_str   = _format_expires_at(expires_raw) if expires_raw else "—"
        location      = (game or {}).get("location")
        max_players   = (game or {}).get("max_players")
        players_line  = f"👥 Players: {total_players}" + (f" / {max_players} spots" if max_players else "")

        caption = (
            "🎮 *New SHAMO Game is Live\\!*\n\n"
            f"🏢 Sponsored by: {company_name or 'SHAMO'}\n"
            f"💰 Prize Pool: {prize_pool:,.0f} ETB\n"
        )
        if location:
            caption += f"📍 Location: {location}\n"
        caption += f"⏰ Expires: {expires_str}\n\nScan the QR code below to join 👆"

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(text="🎯 Play Now", web_app=WebAppInfo(url=SHAMO_WEBAPP_URL))
        ]])
        qr_image_url = (qr or {}).get("qr_image_url")
        use_photo = bool(qr_image_url)

        for i, user in enumerate(users):
            tg_id = user.get("telegram_id")
            if tg_id is None:
                continue
            try:
                if use_photo:
                    msg = await bot.send_photo(chat_id=tg_id, photo=qr_image_url,
                                               caption=caption, reply_markup=keyboard,
                                               parse_mode="Markdown")
                else:
                    msg = await bot.send_message(chat_id=tg_id,
                                                 text=caption.replace("\\!", "!"),
                                                 reply_markup=keyboard, parse_mode="Markdown")
                sent += 1
                _schedule_delete_message(bot, tg_id, msg.message_id, AUTO_DELETE_DELAY)
            except Forbidden:
                blocked += 1
            except BadRequest:
                failed += 1
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    if use_photo:
                        msg = await bot.send_photo(chat_id=tg_id, photo=qr_image_url,
                                                   caption=caption, reply_markup=keyboard,
                                                   parse_mode="Markdown")
                    else:
                        msg = await bot.send_message(chat_id=tg_id,
                                                     text=caption.replace("\\!", "!"),
                                                     reply_markup=keyboard, parse_mode="Markdown")
                    sent += 1
                    _schedule_delete_message(bot, tg_id, msg.message_id, AUTO_DELETE_DELAY)
                except Exception:
                    failed += 1
            except Exception as e:
                logger.warning("Broadcast QR: failed for tg_id=%s: %s", tg_id, e)
                failed += 1
            await asyncio.sleep(0.05)
            if (i + 1) % 50 == 0:
                logger.info("Broadcast QR progress: %d/%d", i + 1, total)

        logger.info("Broadcast QR done: sent=%d failed=%d blocked=%d", sent, failed, blocked)
        return {"sent": sent, "failed": failed, "blocked": blocked, "total": total}
    except Exception as e:
        logger.error("Broadcast QR failed: %s", e)
        return {"sent": 0, "failed": 0, "blocked": 0, "total": 0}


# ─── Build app ───────────────────────────────────────────────
def build_application() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in .env")

    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start",  start))
    application.add_handler(CommandHandler("play",   play_cmd))
    application.add_handler(CommandHandler("help",   help_cmd))
    application.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    application.add_handler(MessageHandler(filters.COMMAND, unknown_cmd))
    application.add_error_handler(error_handler)
    set_bot_app(application)
    return application


# ─── Standalone run ─────────────────────────────────────────
def main() -> None:
    if not acquire_instance_lock():
        logger.error(
            "Another bot instance is already running (port %d is in use). Exiting.",
            _LOCK_PORT,
        )
        sys.exit(1)

    logger.info("Instance lock acquired on port %d.", _LOCK_PORT)
    application = build_application()
    logger.info("SHAMO bot starting (standalone)… Mini App: %s API: %s", SHAMO_WEBAPP_URL, API_BASE_URL)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
