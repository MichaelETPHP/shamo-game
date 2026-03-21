"""Redis helpers for SHAMO — optional; API works without Redis (REDIS_DISABLED=1 or no server)."""
import os
import json
import logging

logger = logging.getLogger("shamo.redis")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379").strip()
# Set REDIS_DISABLED=1 in .env if you do not run Redis (database still works).
REDIS_DISABLED = os.environ.get("REDIS_DISABLED", "").strip().lower() in ("1", "true", "yes", "on")

r = None
try:
    if not REDIS_DISABLED:
        import redis as _redis_mod

        r = _redis_mod.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=2.5,
            socket_timeout=2.5,
        )
except Exception as e:
    logger.warning("Redis not available (%s). Caching/live counts disabled.", e)
    r = None


def _ok():
    return r is not None


def test_connection():
    """Test if Redis is working"""
    if not _ok():
        logger.debug("Redis disabled or not installed")
        return False
    try:
        r.ping()
        return True
    except Exception as e:
        logger.warning("Redis ping failed: %s", e)
        return False


# ─── LIVE PLAYER COUNT ───
def increment_active_players():
    if not _ok():
        return 0
    try:
        return r.incr("shamo:active_players")
    except Exception as e:
        logger.debug("increment_active_players: %s", e)
        return 0


def decrement_active_players():
    if not _ok():
        return 0
    try:
        count = r.decr("shamo:active_players")
        if count < 0:
            r.set("shamo:active_players", 0)
            return 0
        return count
    except Exception as e:
        logger.debug("decrement_active_players: %s", e)
        return 0


def get_active_players():
    if not _ok():
        return 0
    try:
        return int(r.get("shamo:active_players") or 0)
    except Exception as e:
        logger.debug("get_active_players: %s", e)
        return 0


# ─── GAME SESSION ───
def create_game_session(user_id, session_data):
    if not _ok():
        return
    try:
        key = f"shamo:session:{user_id}"
        r.setex(key, 1800, json.dumps(session_data))
    except Exception as e:
        logger.debug("create_game_session: %s", e)


def get_game_session(user_id):
    if not _ok():
        return None
    try:
        key = f"shamo:session:{user_id}"
        data = r.get(key)
        return json.loads(data) if data else None
    except Exception as e:
        logger.debug("get_game_session: %s", e)
        return None


def delete_game_session(user_id):
    if not _ok():
        return
    try:
        r.delete(f"shamo:session:{user_id}")
    except Exception as e:
        logger.debug("delete_game_session: %s", e)


# ─── LEADERBOARD ───
def update_leaderboard(user_id, username, score):
    if not _ok():
        return
    try:
        r.zadd("shamo:leaderboard", {f"{user_id}:{username}": score})
    except Exception as e:
        logger.debug("update_leaderboard: %s", e)


def get_leaderboard(top=10):
    if not _ok():
        return []
    try:
        results = r.zrevrange("shamo:leaderboard", 0, top - 1, withscores=True)
        leaderboard = []
        for member, score in results:
            user_id, username = member.split(":", 1)
            leaderboard.append({
                "user_id": user_id,
                "username": username,
                "score": int(score),
            })
        return leaderboard
    except Exception as e:
        logger.debug("get_leaderboard: %s", e)
        return []


# ─── RATE LIMITING ───
def check_rate_limit(user_id, max_requests=5, window=60):
    if not _ok():
        return True  # allow when Redis off
    try:
        key = f"shamo:rate:{user_id}"
        current = r.get(key)
        if current and int(current) >= max_requests:
            return False
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        pipe.execute()
        return True
    except Exception as e:
        logger.debug("check_rate_limit: %s", e)
        return True


# ─── PRIZE POOL CACHE ───
def set_prize_pool(pool_id, amount):
    if not _ok():
        return
    try:
        r.set(f"shamo:pool:{pool_id}", str(amount))
    except Exception as e:
        logger.debug("set_prize_pool: %s", e)


def get_prize_pool(pool_id):
    if not _ok():
        return None
    try:
        amount = r.get(f"shamo:pool:{pool_id}")
        return float(amount) if amount else None
    except Exception as e:
        logger.debug("get_prize_pool: %s", e)
        return None


def deduct_from_pool(pool_id, amount):
    if not _ok():
        return None
    try:
        current = get_prize_pool(pool_id)
        if current and current >= amount:
            new_amount = current - amount
            set_prize_pool(pool_id, new_amount)
            return new_amount
        return None
    except Exception as e:
        logger.debug("deduct_from_pool: %s", e)
        return None


def clear_prize_pool_cache(pool_id: str) -> None:
    """Remove cached prize pool countdown/value for a game (pool_id is usually game UUID)."""
    if not _ok():
        return
    try:
        r.delete(f"shamo:pool:{pool_id}")
    except Exception as e:
        logger.debug("clear_prize_pool_cache: %s", e)
