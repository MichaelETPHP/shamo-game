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

## Where to integrate real services

- **Withdrawals** — handled via Telebirr; configure in admin panel and `.env`.

- **Telegram Bot mini-app / deep links**
  - When used inside Telegram, you can:
    - Generate a unique game link per user from your Bot.
    - Attach a token or session ID as a URL param.
    - Read that token in `app.js` and enforce **one spin per token**.

- **Supabase (quiz + spin history)**
  - Persist:
    - Player identifier (e.g. Telegram user id or bot-issued token)
    - Quiz questions served + answers + score
    - Final spin result + timestamp
  - Before letting a user spin:
    - Query Supabase for any existing, unspent game for that player.
    - If found, show a "You already played this round" state instead of the quiz.

- **Creator-funded $10 pools**
  - At creation time, store in Supabase:
    - Creator id
    - Pool amount (e.g. $10)
    - Prize distribution
    - Custom question set
  - When loading the game:
    - Fetch the pool config by pool id
    - Use its prize distribution + question set instead of the hard-coded ones in `app.js`.

# shamo-game
