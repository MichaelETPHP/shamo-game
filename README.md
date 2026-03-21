# SHAMO Prize Spinner (Simulation)

A single-page SHAMO sandbox that walks through the full experience:

- Lobby with live player count and $10 pool breakdown
- 3-question Ethiopian culture quiz (4s per question, 1 retry, must score 3/3)
- Classic 10-segment weighted prize wheel ($0.10 → $5.00) with smooth spin
- Result screen with withdrawal via Telebirr (configure in admin)

## Running the sandbox

You now have a **pure HTML/CSS/JS** front-end – no Node, no npm.

- Easiest: just double‑click `index.html` and open it in your browser.
- Or, use Python’s built‑in static server:

  ```bash
  cd C:\PY-BOT\AI-PROJECT\Shamo
  python -m http.server 8000
  ```

  Then open `http://localhost:8000` in your browser.

The app is completely static (no bundler, no npm dependencies).

## Running the Telegram bot (Python)

1. **Install dependencies**

   ```powershell
   cd C:\PY-BOT\AI-PROJECT\Shamo
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. **Set your bot token (from BotFather)**

   ```powershell
   setx TELEGRAM_BOT_TOKEN "YOUR_TELEGRAM_BOT_TOKEN_HERE"
   ```

   Then open a **new** terminal so the variable is picked up.

3. **Start the bot**

   ```powershell
   cd C:\PY-BOT\AI-PROJECT\Shamo
   .\.venv\Scripts\Activate.ps1
   python bot.py
   ```

4. **What the bot does**

   - `/start` → asks user to **share phone** via Telegram contact button.
   - On contact → logs `(user_id, phone)` and sends an inline button **“Play SHAMO”** that opens your Mini App URL (set in `SHAMO_WEBAPP_URL` in `bot.py`).
   - Uses **long polling**, so you don’t need webhooks while testing.

> Note: for the Mini App to work inside Telegram, `SHAMO_WEBAPP_URL` must point to an HTTPS-hosted copy of this app, not `localhost`.

## Environment (one place)

All database-related variables are read from `.env` in **`shamo_env.py`**:

| Variable | Used for |
|----------|----------|
| `SUPABASE_URL` | PostgREST / Supabase HTTP API (`supabase-py`, mini-app `fetch` to `/rest/v1`) |
| `SUPABASE_KEY` or `SUPABASE_ANON_KEY` | Anon JWT injected into the mini-app |
| `SUPABASE_SERVICE_KEY` | Service-role JWT for the Python API (server) |
| `DATABASE_URL` | Direct PostgreSQL (`db.py`, migration scripts) |
| `DB_SCHEMA` | `search_path` for raw SQL (default `public`) |

Copy **`ENV_SELF_HOSTED.sample`** as a template. `SUPABASE_URL` must be your **API** URL (where `/rest/v1` lives), not only the `db.*` Postgres host.

### One-shot schema + default settings + sample rows

Run **`migrations/shamo_one_click_install_and_seed.sql`** in the Supabase SQL editor (or `psql`) on a **new** database or empty `shamo` schema. It:

- Creates schema **`shamo`**, enums, all tables, indexes, triggers, views, RLS policies
- Upserts **`platform_config`** defaults (fees, withdrawal limits, `usd_to_etb_rate`, etc.)
- Inserts admin user **`telegram_id = 0`** (for API “created_by” lookups) and **15** approved quiz questions + answers
- Adds **sample** player, company **`sample-roasters-plc`**, confirmed deposit, **draft** game, **3 `game_questions`**, and an **inactive** QR stub

Expose the **`shamo`** schema to PostgREST (Supabase: *Settings → API → Exposed schemas*) or align `DB_SCHEMA` / API with your host’s schema.

## Where to integrate real services

- **Withdrawals** — handled via Telebirr; configure in admin panel and `.env`.

- **Telegram Bot mini-app / deep links**
  - When used inside Telegram, you can:
    - Generate a unique game link per user from your Bot.
    - Attach a token or session ID as a URL param.
    - Read that token in `app.js` and enforce **one spin per token**.

- **PostgreSQL (quiz + spin history)**
  - Persist via the server API backed by PostgreSQL (set `DATABASE_URL` in `.env`):
    - Player identifier (e.g. Telegram user id or bot-issued token)
    - Quiz questions served + answers + score
    - Final spin result + timestamp
  - Before letting a user spin:
    - Query the API for any existing, unspent game for that player.
    - If found, show a "You already played this round" state instead of the quiz.

- **Creator-funded $10 pools**
  - At creation time, store in the database via the API:
    - Creator id
    - Pool amount (e.g. $10)
    - Prize distribution
    - Custom question set
  - When loading the game:
    - Fetch the pool config by pool id
    - Use its prize distribution + question set instead of the hard-coded ones in `app.js`.

# shamo-game
