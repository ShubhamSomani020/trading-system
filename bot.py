import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from logic import log_trade, get_stock_history, calculate_pnl, add_to_watchlist, get_watchlist, get_stock_info
from database import init_db
from datetime import date

init_db()

TOKEN = "8713736731:AAHFFbC-CMEcBKBjmr5cs9VcA0ni7HvQEA4"

# ── /start ─────────────────────────────────────────────
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = """
🤖 *Trading Bot Ready*

*TRADE COMMANDS:*
/buy SYMBOL PRICE QTY notes
/sell SYMBOL PRICE QTY notes

*INFO COMMANDS:*
/history SYMBOL
/pnl SYMBOL
/info SYMBOL

*WATCHLIST:*
/watch SYMBOL TARGET STOPLOSS reason
/watchlist

*ALERTS:*
/alert SYMBOL PRICE above/below

Type any command to begin.
    """
    await update.message.reply_text(msg, parse_mode="Markdown")

# ── /buy ───────────────────────────────────────────────
async def buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        args = ctx.args
        symbol = args[0]
        price = float(args[1])
        qty = int(args[2])
        notes = " ".join(args[3:]) if len(args) > 3 else ""
        msg = log_trade(symbol, "BUY", price, qty, str(date.today()), notes, "Disciplined", "")
        await update.message.reply_text(f"✅ *{msg}*\n📅 {date.today()}\n💰 ₹{price} x {qty} = ₹{price*qty:,.0f}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text("❌ Format: /buy SYMBOL PRICE QTY notes\nExample: /buy TCS 4200 10 good entry")

# ── /sell ──────────────────────────────────────────────
async def sell(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        args = ctx.args
        symbol = args[0]
        price = float(args[1])
        qty = int(args[2])
        notes = " ".join(args[3:]) if len(args) > 3 else ""
        msg = log_trade(symbol, "SELL", price, qty, str(date.today()), notes, "Disciplined", "")
        await update.message.reply_text(f"✅ *{msg}*\n📅 {date.today()}\n💰 ₹{price} x {qty} = ₹{price*qty:,.0f}", parse_mode="Markdown")
    except:
        await update.message.reply_text("❌ Format: /sell SYMBOL PRICE QTY notes\nExample: /sell TCS 4500 10 target hit")

# ── /history ───────────────────────────────────────────
async def history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        symbol = ctx.args[0].upper()
        df = get_stock_history(symbol)
        if df.empty:
            await update.message.reply_text(f"No history for {symbol}")
            return
        msg = f"📊 *{symbol} - Last 5 Trades*\n\n"
        for _, row in df.head(5).iterrows():
            msg += f"{'🟢' if row['trade_type']=='BUY' else '🔴'} {row['trade_type']} | ₹{row['price']} x {row['quantity']} | {row['date']}\n"
            if row['notes']:
                msg += f"   📝 {row['notes']}\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except:
        await update.message.reply_text("❌ Format: /history SYMBOL")

# ── /pnl ───────────────────────────────────────────────
async def pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        symbol = ctx.args[0].upper()
        result = calculate_pnl(symbol)
        if not result:
            await update.message.reply_text(f"No trades found for {symbol}")
            return
        emoji = "🟢" if result['pnl'] >= 0 else "🔴"
        msg = f"""
{emoji} *{symbol} P&L Summary*

💰 Invested: ₹{result['invested']:,.0f}
💵 Returned: ₹{result['returned']:,.0f}
📈 P&L: ₹{result['pnl']:,.0f}
📊 Return: {result['pnl_pct']:.1f}%
        """
        await update.message.reply_text(msg, parse_mode="Markdown")
    except:
        await update.message.reply_text("❌ Format: /pnl SYMBOL")

# ── /info ──────────────────────────────────────────────
async def info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        symbol = ctx.args[0].upper()
        row = get_stock_info(symbol)
        if not row:
            await update.message.reply_text(f"{symbol} not in database.")
            return
        msg = f"""
📋 *{symbol} - {row[2]}*

🏭 Sector: {row[3]}
🟢 Support: ₹{row[4]}
🔴 Resistance: ₹{row[5]}
📝 Notes: {row[6]}
        """
        await update.message.reply_text(msg, parse_mode="Markdown")
    except:
        await update.message.reply_text("❌ Format: /info SYMBOL")

# ── /watch ─────────────────────────────────────────────
async def watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        args = ctx.args
        symbol = args[0]
        target = float(args[1])
        sl = float(args[2])
        reason = " ".join(args[3:]) if len(args) > 3 else ""
        msg = add_to_watchlist(symbol, target, sl, reason)
        await update.message.reply_text(f"✅ {msg}\n🎯 Target: ₹{target}\n🛑 SL: ₹{sl}", parse_mode="Markdown")
    except:
        await update.message.reply_text("❌ Format: /watch SYMBOL TARGET STOPLOSS reason\nExample: /watch TCS 4500 3900 breakout setup")

# ── /watchlist ─────────────────────────────────────────
async def watchlist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    wl = get_watchlist()
    if wl.empty:
        await update.message.reply_text("Watchlist is empty.")
        return
    msg = "👀 *Your Watchlist*\n\n"
    for _, row in wl.iterrows():
        msg += f"📌 *{row['symbol']}*\n"
        msg += f"   🎯 Target: ₹{row['target_price']} | 🛑 SL: ₹{row['stop_loss']}\n"
        if row['reason']:
            msg += f"   📝 {row['reason']}\n"
        msg += "\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

# ── /alert ─────────────────────────────────────────────
# Stores alerts in memory (simple version)
alerts = []

async def alert(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        args = ctx.args
        symbol = args[0].upper()
        price = float(args[1])
        direction = args[2].lower()  # above or below
        alerts.append({
            "symbol": symbol,
            "price": price,
            "direction": direction,
            "chat_id": update.message.chat_id
        })
        await update.message.reply_text(
            f"🔔 Alert set!\n📌 *{symbol}* — notify when price goes *{direction}* ₹{price}",
            parse_mode="Markdown"
        )
    except:
        await update.message.reply_text("❌ Format: /alert SYMBOL PRICE above/below\nExample: /alert TCS 4500 above")

# ── RUN BOT ────────────────────────────────────────────
def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy))
    app.add_handler(CommandHandler("sell", sell))
    app.add_handler(CommandHandler("history", history))
    app.add_handler(CommandHandler("pnl", pnl))
    app.add_handler(CommandHandler("info", info))
    app.add_handler(CommandHandler("watch", watch))
    app.add_handler(CommandHandler("watchlist", watchlist))
    app.add_handler(CommandHandler("alert", alert))
    print("🤖 Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()