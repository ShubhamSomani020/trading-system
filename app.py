import streamlit as st
import pandas as pd
from sheets_db import (
    add_stock, get_all_stocks, get_stock_info,
    log_trade, get_stock_history, calculate_pnl,
    add_to_watchlist, get_watchlist
)

st.set_page_config(page_title="My Trading System", layout="wide")
st.title("📈 My Trading System")

menu = st.sidebar.radio("Navigate", [
    "🏠 Dashboard",
    "➕ Add Stock",
    "📝 Log Trade",
    "🔍 Stock History",
    "👀 Watchlist"
])

# ── DASHBOARD ──────────────────────────────────────────
if menu == "🏠 Dashboard":
    st.subheader("Portfolio Overview")
    trades = get_stock_history("")
    if trades.empty:
        st.info("No trades logged yet. Add a stock and log your first trade.")
    else:
        symbols = trades["symbol"].unique()
        for sym in symbols:
            pnl = calculate_pnl(sym)
            if pnl:
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Stock", sym)
                col2.metric("Invested", f"₹{pnl['invested']:,.0f}")
                col3.metric("P&L", f"₹{pnl['pnl']:,.0f}")
                col4.metric("Return %", f"{pnl['pnl_pct']:.1f}%")
                st.divider()

# ── ADD STOCK ──────────────────────────────────────────
elif menu == "➕ Add Stock":
    st.subheader("Add Stock to Database")
    col1, col2 = st.columns(2)
    with col1:
        symbol  = st.text_input("Symbol (e.g. TCS)")
        name    = st.text_input("Company Name")
        sector  = st.text_input("Sector")
    with col2:
        support  = st.number_input("Support Zone ₹", value=0.0)
        resist   = st.number_input("Resistance Zone ₹", value=0.0)
    notes = st.text_area("Study Notes / Your Thesis")

    if st.button("Add Stock"):
        if symbol:
            msg = add_stock(symbol, name, sector, support, resist, notes)
            st.success(msg)
        else:
            st.warning("Enter a symbol first.")

    st.divider()
    st.subheader("All Stocks in Database")
    all_stocks = get_all_stocks()
    if not all_stocks.empty:
        st.dataframe(all_stocks, use_container_width=True)

# ── LOG TRADE ──────────────────────────────────────────
elif menu == "📝 Log Trade":
    st.subheader("Log a Trade")
    col1, col2 = st.columns(2)
    with col1:
        symbol     = st.text_input("Symbol")
        trade_type = st.selectbox("Type", ["BUY", "SELL"])
        price      = st.number_input("Price ₹", value=0.0)
        quantity   = st.number_input("Quantity", value=1, step=1)
    with col2:
        date    = st.date_input("Date")
        emotion = st.selectbox("Emotion", ["Disciplined", "FOMO", "Panic", "Greedy", "Neutral"])
        mistake = st.text_input("Mistake (if any)")
    notes = st.text_area("Trade Notes")

    if st.button("Log Trade"):
        if symbol:
            msg = log_trade(symbol, trade_type, price, quantity, str(date), notes, emotion, mistake)
            st.success(msg)
        else:
            st.warning("Enter a symbol first.")

# ── STOCK HISTORY ──────────────────────────────────────
elif menu == "🔍 Stock History":
    st.subheader("Stock History & Study Notes")
    symbol = st.text_input("Enter Symbol")

    if symbol:
        info = get_stock_info(symbol)
        if info:
            col1, col2, col3 = st.columns(3)
            col1.metric("Support Zone", f"₹{info['support_zone']}")
            col2.metric("Resistance Zone", f"₹{info['resistance_zone']}")
            col3.write(f"**Sector:** {info['sector']}")
            st.info(f"📝 Study Notes: {info['study_notes']}")
        else:
            st.warning("Stock not in database. Add it first.")

        history = get_stock_history(symbol)
        if not history.empty:
            st.subheader("Trade History")
            st.dataframe(history, use_container_width=True)

            pnl = calculate_pnl(symbol)
            if pnl:
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Invested", f"₹{pnl['invested']:,.0f}")
                col2.metric("Total Returned", f"₹{pnl['returned']:,.0f}")
                col3.metric("P&L", f"₹{pnl['pnl']:,.0f}", f"{pnl['pnl_pct']:.1f}%")
        else:
            st.info("No trade history found for this stock.")

# ── WATCHLIST ──────────────────────────────────────────
elif menu == "👀 Watchlist":
    st.subheader("Watchlist")

    with st.expander("➕ Add to Watchlist"):
        col1, col2 = st.columns(2)
        with col1:
            sym    = st.text_input("Symbol")
            target = st.number_input("Target ₹", value=0.0)
        with col2:
            sl     = st.number_input("Stop Loss ₹", value=0.0)
            reason = st.text_area("Reason / Setup")
        if st.button("Add to Watchlist"):
            if sym:
                st.success(add_to_watchlist(sym, target, sl, reason))
            else:
                st.warning("Enter a symbol.")

    st.divider()
    wl = get_watchlist()
    if not wl.empty:
        st.dataframe(wl, use_container_width=True)
    else:
        st.info("Watchlist is empty.")