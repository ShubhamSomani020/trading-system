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
NIFTY500_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "BHARTIARTL", "ICICIBANK",
    "INFOSYS", "SBIN", "HINDUNILVR", "ITC", "LT",
    "KOTAKBANK", "AXISBANK", "BAJFINANCE", "MARUTI", "ASIANPAINT",
    "TITAN", "SUNPHARMA", "ULTRACEMCO", "NESTLEIND", "WIPRO",
    "ONGC", "NTPC", "POWERGRID", "COALINDIA", "HCLTECH",
    "BAJAJFINSV", "TECHM", "TATAMOTORS", "ADANIENT", "ADANIPORTS",
    "JSWSTEEL", "TATASTEEL", "HINDALCO", "CIPLA", "DRREDDY",
    "DIVISLAB", "APOLLOHOSP", "EICHERMOT", "BAJAJ-AUTO", "HEROMOTOCO",
    "BRITANNIA", "DABUR", "MARICO", "COLPAL", "GODREJCP",
    "PIDILITIND", "BERGEPAINT", "HAVELLS", "VOLTAS", "WHIRLPOOL",
    "SIEMENS", "ABB", "BOSCHLTD", "CUMMINSIND", "THERMAX",
    "MOTHERSON", "BALKRISIND", "APOLLOTYRE", "MRF", "EXIDEIND",
    "TATACONSUM", "MCDOWELL-N", "UNITDSPR", "RADICO", "UBL",
    "PAGEIND", "VEDL", "NMDC", "NATIONALUM", "HINDCOPPER",
    "GRASIM", "AMBUJACEM", "ACC", "SHREECEM", "DALMIACEMT",
    "JKCEMENT", "RAMCOCEM", "STARCEMENT", "INDIACEM", "HEIDELBERG",
    "INDUSINDBK", "FEDERALBNK", "IDFCFIRSTB", "BANDHANBNK", "RBLBANK",
    "PNB", "BANKBARODA", "CANBK", "UNIONBANK", "INDIANB",
    "CHOLAFIN", "MUTHOOTFIN", "BAJAJHLDNG", "LICHSGFIN", "PNBHOUSING",
    "HDFCLIFE", "SBILIFE", "ICICIPRULI", "MAXFINSERV", "STARHEALTH",
    "NAUKRI", "INFOEDGE", "INDIAMART", "JUSTDIAL", "AFFLE",
    "ROUTE", "TATAELXSI", "LTTS", "PERSISTENT", "COFORGE",
    "MINDTREE", "MPHASIS", "HEXAWARE", "NIITTECH", "KPITTECH",
    "ZOMATO", "PAYTM", "NYKAA", "POLICYBZR", "CARTRADE",
    "DELHIVERY", "MAPMYINDIA", "IRCTC", "RAILTEL", "RVNL",
    "IRFC", "RECLTD", "PFC", "HUDCO", "NBCC",
    "DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "BRIGADE",
    "PHOENIXLTD", "SOBHA", "MAHLIFE", "SUNTECK", "KOLTEPATIL",
    "HAVELLS", "POLYCAB", "KEI", "FINOLEX", "HLEGLAS",
    "DIXON", "AMBER", "BLUESTAR", "DAIKININD", "LLOYDSENGG",
    "TRENT", "AVENUE", "VMART", "SHOPERSTOP", "ABFRL",
    "VEDANT", "MANYAVAR", "PVRINOX", "INOXLEISUR", "ZEEL",
    "SUNTV", "NETWORK18", "TVTODAY", "JAGRAN", "DBCORP",
    "AARTIIND", "DEEPAKNTR", "NAVINFLUOR", "SRF", "ATUL",
    "FLUOROCHEM", "ALKYLAMINE", "FINEORG", "TATACHEM", "GHCL",
    "TATAPOWER", "ADANIGREEN", "ADANIENWRG", "CESC", "TORNTPOWER",
    "JSWENERGY", "GREENKO", "RPOWER", "NHPC", "SJVN",
    "BPCL", "IOC", "HINDPETRO", "MRPL", "CASTROLIND",
    "GAIL", "IGL", "MGL", "GSPL", "PETRONET",
    "CONCOR", "BLUEDART", "GATI", "TCI", "MAHINDCIE",
    "ESCORTS", "FORCEMOT", "TIINDIA", "SCHAEFFLER", "SKFINDIA",
    "ASTRAL", "SUPREMEIND", "FINOLEX", "PRINCEPIPE", "KANSAINER",
    "AKZOINDIA", "SHALPAINTS", "VINATIORGA", "BASF", "NOCIL",
    "IPCALAB", "ALKEM", "TORNTPHARM", "AUROPHARMA", "LUPIN",
    "GLENMARK", "BIOCON", "ABBOTINDIA", "PFIZER", "SANOFI",
    "GLAXO", "JBCHEPHARM", "NATCOPHARM", "GRANULES", "LAURUSLABS",
    "METROPOLIS", "THYROCARE", "KRSNAA", "VIJAYADIAG", "HEALTHINDIA",
    "FORTIS", "MAXHEALTH", "NARAYANHA", "ASTER", "KIMS",
    "ZYDUSLIFE", "MANKIND", "PIRAMALPHA", "SOLARA", "DIVI",
    "IDEA", "TATACOMM", "HFCL", "STLTECH", "TEJAS",
    "TANLA", "ONMOBILE", "LATENTVIEW", "MASTEK", "ZENSAR",
    "ECLERX", "RATEGAIN", "SAPIENT", "BIRLASOFT", "CYIENT",
    "LTIMINDTREE", "OFSS", "FSL", "CDSL", "BSE",
    "MCX", "ANGELONE", "ICICIGI", "GODIGIT", "SBICARDS",
    "MANAPPURAM", "AAVAS", "APTUS", "HOMEFIRST", "CREDITACC",
    "SPANDANA", "UJJIVANSFB", "EQUITASBNK", "SURYODAY", "ESAFSFB",
    "SOUTHBANK", "KARNATAKABK", "CITYUNIONBK", "DCBBANK", "LAKSHVILAS",
    "HDFCAMC", "NIPPONLIFE", "UTIAMC", "360ONE", "ISEC",
    "MOTILALOFS", "EDELWEISS", "IFCI", "IDFC", "JMFINANCIL",
    "CHOLAHLDNG", "SUNDARMFIN", "MMFIN", "SHRIRAMFIN", "SBFC",
    "UGROCAP", "AROHAN", "FUSION", "SATIN", "VASTU",
    "TATAINVEST", "BFINVEST", "PILANIINVS", "TATAELXSI", "ZENTEC",
    "INDIGOPNTS", "SIGNATURE", "APTECHT", "NUCLEUS", "INTELLECT",
    "SUBEXLTD", "RSYSTEMS", "SAKSOFT", "DATAMATICS", "SIXTHSENSE",
    "GPIL", "JSPL", "SAIL", "JINDALSAW", "WELSPUNIND",
    "RATNAMANI", "APL", "MAHSEAMLES", "SURYA", "GRAVITA",
    "VEDL", "HINDZINC", "HINDCOPPER", "MOIL", "GMRAIRPORT",
    "ADANIAIRPT", "HAL", "BEL", "BEML", "MIDHANI",
    "MAZDOCK", "COCHINSHIP", "GRSE", "BHARATFORG", "KALYANKJIL",
    "PCJEWELLER", "RAJESHEXPO", "GITANJALI", "THANGAMAYL", "TITAN",
    "SULA", "GLOBUSSPR", "SAPPHIRE", "ONWARD", "CAMPUS",
    "BATAINDIA", "RELAXO", "LIBERTY", "MIRZA", "KHADIM",
    "SYMPHONY", "HAWKINCOOK", "TTK", "VSTIND", "GODFRYPHLP",
    "RADIANTCMS", "IOLCP", "SUVEN", "SMSPHARMA", "SEQUENT",
    "PGHL", "CAPLIPOINT", "GLAND", "STRIDES", "SHILPAMED",
    "RAINBOW", "MEDPLUS", "TIPSFILMS", "SAREGAMA", "TIPS",
    "IMAGICAA", "WONDERLA", "MAHINDRA", "MFSL", "ABSLAMC"
]

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