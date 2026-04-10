import asyncio
import threading
import time
import yfinance as yf
import pandas as pd
from datetime import datetime
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

SCANNER_TOKEN = "8641739845:AAEOwZQ3291-Oxpvo44KMVZQvfNd9pZIS5U"
CHAT_ID = "8572354616"
BROADCAST_INTERVAL = 15 * 60  # 15 minutes in seconds

# ── NIFTY 500 SYMBOLS ──────────────────────────────────
# Full Nifty 500 list — yfinance format (.NS suffix handled in code)
from sheets_db import get_nifty500_symbols, get_active_alerts, mark_alert_triggered, get_live_price
NIFTY500_SYMBOLS = get_nifty500_symbols()
print(f"Loaded {len(NIFTY500_SYMBOLS)} symbols from sheet")

# ── SHARED PRICE CACHE ─────────────────────────────────
price_cache = {}
cache_lock = threading.Lock()

# ── FETCH IN BACKGROUND ────────────────────────────────
def fetch_all_prices():
    """Continuously fetches Nifty 500 prices in batches."""
    batch_size = 50
    symbols = NIFTY500_SYMBOLS

    while True:
        try:
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                tickers = [f"{s}.NS" for s in batch]
                try:
                    data = yf.download(
                        tickers,
                        period="5d",
                        interval="1d",
                        group_by="ticker",
                        auto_adjust=True,
                        progress=False,
                        threads=True
                    )
                    for sym in batch:
                        try:
                            ticker = f"{sym}.NS"
                            if len(tickers) == 1:
                                closes = data["Close"]
                            else:
                                closes = data[ticker]["Close"]

                            closes = closes.dropna()
                            if len(closes) >= 2:
                                today_close  = closes.iloc[-1]
                                prev_close   = closes.iloc[-2]
                                pct_change   = ((today_close - prev_close) / prev_close) * 100

                                # volume
                                if len(tickers) == 1:
                                    volumes = data["Volume"].dropna()
                                else:
                                    volumes = data[ticker]["Volume"].dropna()

                                today_vol  = volumes.iloc[-1]
                                avg_vol_5d = volumes.iloc[-5:].mean() if len(volumes) >= 5 else volumes.mean()
                                vol_ratio  = today_vol / avg_vol_5d if avg_vol_5d > 0 else 1.0

                                with cache_lock:
                                    price_cache[sym] = {
                                        "ltp":        round(today_close, 2),
                                        "prev_close": round(prev_close, 2),
                                        "pct_change": round(pct_change, 2),
                                        "today_vol":  int(today_vol),
                                        "avg_vol_5d": int(avg_vol_5d),
                                        "vol_ratio":  round(vol_ratio, 2),
                                        "updated_at": datetime.now().strftime("%H:%M:%S")
                                    }
                        except Exception:
                            continue
                except Exception as e:
                    print(f"Batch fetch error: {e}")

                time.sleep(2)  # small gap between batches to avoid rate limit

        except Exception as e:
            print(f"Fetch loop error: {e}")

        time.sleep(30)  # restart full cycle after 30s gap

# ── BUILD SNAPSHOT ─────────────────────────────────────
def build_snapshot():
    with cache_lock:
        data = dict(price_cache)

    if not data:
        return None

    df = pd.DataFrame(data).T
    df = df.apply(pd.to_numeric, errors="ignore")

    # Top 10 gainers
    gainers = df.nlargest(10, "pct_change")[["ltp", "pct_change"]]

    # Top 10 losers
    losers = df.nsmallest(10, "pct_change")[["ltp", "pct_change"]]

    # Top 5 volume surgers (today vol >> 5d avg)
    vol_surgers = df.nlargest(5, "vol_ratio")[["ltp", "today_vol", "avg_vol_5d", "vol_ratio"]]

    # Bottom 5 volume drainers
    vol_drainers = df.nsmallest(5, "vol_ratio")[["ltp", "today_vol", "avg_vol_5d", "vol_ratio"]]

    return gainers, losers, vol_surgers, vol_drainers

# ── FORMAT MESSAGE ─────────────────────────────────────
def format_snapshot_msg():
    result = build_snapshot()
    if not result:
        return "⏳ Still loading data, try again in a minute."

    gainers, losers, vol_surgers, vol_drainers = result
    now = datetime.now().strftime("%d %b %Y %H:%M")

    msg = f"📊 *Market Snapshot — {now}*\n"
    msg += f"_Nifty 500 | {len(price_cache)} stocks loaded_\n\n"

    msg += "🟢 *Top 10 Gainers*\n"
    for sym, row in gainers.iterrows():
        msg += f"  `{sym:<15}` ₹{row['ltp']:>8.2f}  +{row['pct_change']:.2f}%\n"

    msg += "\n🔴 *Top 10 Losers*\n"
    for sym, row in losers.iterrows():
        msg += f"  `{sym:<15}` ₹{row['ltp']:>8.2f}  {row['pct_change']:.2f}%\n"

    msg += "\n📈 *Top 5 Volume Surgers*\n"
    for sym, row in vol_surgers.iterrows():
        msg += f"  `{sym:<15}` {row['vol_ratio']:.1f}x avg vol\n"

    msg += "\n📉 *Bottom 5 Volume Drainers*\n"
    for sym, row in vol_drainers.iterrows():
        msg += f"  `{sym:<15}` {row['vol_ratio']:.1f}x avg vol\n"

    return msg

# ── /start ─────────────────────────────────────────────
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = """
🔭 *Market Scanner Bot*

/scan — get latest snapshot now
/status — how many stocks loaded

Auto-broadcasts every 15 min during market hours.
    """
    await update.message.reply_text(msg, parse_mode="Markdown")

# ── /scan ──────────────────────────────────────────────
async def scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Building snapshot...")
    msg = format_snapshot_msg()
    await update.message.reply_text(msg, parse_mode="Markdown")

# ── /status ────────────────────────────────────────────
async def status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    count = len(price_cache)
    total = len(NIFTY500_SYMBOLS)
    await update.message.reply_text(
        f"📡 *Scanner Status*\n\n"
        f"Loaded: {count} / {total} stocks\n"
        f"Last update: {list(price_cache.values())[-1]['updated_at'] if price_cache else 'N/A'}",
        parse_mode="Markdown"
    )

# ── 15-MIN BROADCASTER ─────────────────────────────────
def is_market_hours():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    open_  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_ = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_ <= now <= close_

def broadcaster():
    bot  = Bot(token=SCANNER_TOKEN)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        time.sleep(BROADCAST_INTERVAL)
        try:
            if is_market_hours():
                msg = format_snapshot_msg()
                loop.run_until_complete(
                    bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
                )
                print(f"[{datetime.now().strftime('%H:%M')}] Broadcast sent.")
        except Exception as e:
            print(f"Broadcast error: {e}")

# ── MAIN ───────────────────────────────────────────────
def main():
    # background: fetch prices continuously
    t1 = threading.Thread(target=fetch_all_prices, daemon=True)
    t1.start()

    # background: broadcast every 15 min
    t2 = threading.Thread(target=broadcaster, daemon=True)
    t2.start()

    app = ApplicationBuilder().token(SCANNER_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scan", scan))
    app.add_handler(CommandHandler("status", status))

    print("🔭 Scanner bot running...")
    app.run_polling()

if __name__ == "__main__":
    main()