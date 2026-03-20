"""Redis helpers for SHAMO (live counts, sessions, leaderboard, rate limit, pool cache)."""
import os
import json
import redis

# Connect to Redis (lazy connect on first command)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def test_connection():
    """Test if Redis is working"""
    try:
        r.ping()
        print("✅ Redis connected!")
        return True
    except Exception as e:
        print(f"❌ Redis failed: {e}")
        return False


# ─── LIVE PLAYER COUNT ───
def increment_active_players():
    return r.incr("shamo:active_players")


def decrement_active_players():
    count = r.decr("shamo:active_players")
    if count < 0:
        r.set("shamo:active_players", 0)
        return 0
    return count


def get_active_players():
    return int(r.get("shamo:active_players") or 0)


# ─── GAME SESSION ───
def create_game_session(user_id, session_data):
    """Store game session for 30 minutes"""
    key = f"shamo:session:{user_id}"
    r.setex(key, 1800, json.dumps(session_data))  # expires in 30 min


def get_game_session(user_id):
    key = f"shamo:session:{user_id}"
    data = r.get(key)
    return json.loads(data) if data else None


def delete_game_session(user_id):
    r.delete(f"shamo:session:{user_id}")


# ─── LEADERBOARD ───
def update_leaderboard(user_id, username, score):
    """Add/update player score in leaderboard"""
    r.zadd("shamo:leaderboard", {f"{user_id}:{username}": score})


def get_leaderboard(top=10):
    """Get top players"""
    results = r.zrevrange("shamo:leaderboard", 0, top - 1, withscores=True)
    leaderboard = []
    for member, score in results:
        user_id, username = member.split(":", 1)
        leaderboard.append({
            "user_id": user_id,
            "username": username,
            "score": int(score)
        })
    return leaderboard


# ─── RATE LIMITING ───
def check_rate_limit(user_id, max_requests=5, window=60):
    """Allow max_requests per window seconds"""
    key = f"shamo:rate:{user_id}"
    current = r.get(key)
    if current and int(current) >= max_requests:
        return False  # Rate limited
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window)
    pipe.execute()
    return True  # Allowed


# ─── PRIZE POOL CACHE ───
def set_prize_pool(pool_id, amount):
    r.set(f"shamo:pool:{pool_id}", str(amount))


def get_prize_pool(pool_id):
    amount = r.get(f"shamo:pool:{pool_id}")
    return float(amount) if amount else None


def deduct_from_pool(pool_id, amount):
    """Deduct prize amount from pool"""
    current = get_prize_pool(pool_id)
    if current and current >= amount:
        new_amount = current - amount
        set_prize_pool(pool_id, new_amount)
        return new_amount
    return None
