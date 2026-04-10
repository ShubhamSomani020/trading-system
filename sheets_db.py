import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import os
import json

SHEET_ID = "1FE1MvvvHW6wh0gfPxrOdOmya16HPiPQx8XEkv1fepaw"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_PATH = os.path.join(BASE_DIR, "credentials.json")

def get_client():
    if os.path.exists(CREDS_PATH):
        creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    else:
        import streamlit as st
        creds_dict = json.loads(st.secrets["google_credentials"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def get_sheet(tab_name):
    client = get_client()
    sheet = client.open_by_key(SHEET_ID)
    return sheet.worksheet(tab_name)

# ── STOCKS ─────────────────────────────────────────────
def add_stock(symbol, name="", sector="", support=0, resistance=0, notes=""):
    ws = get_sheet("stocks")
    if ws.cell(1,1).value != "symbol":
        ws.append_row(["symbol","name","sector","support_zone","resistance_zone","study_notes","created_at"])
    data = ws.get_all_records()
    for row in data:
        if row["symbol"] == symbol.upper():
            return f"{symbol} already exists."
    ws.append_row([symbol.upper(), name, sector, support, resistance, notes, str(datetime.now())])
    return f"{symbol} added successfully."

def get_all_stocks():
    ws = get_sheet("stocks")
    data = ws.get_all_records()
    return pd.DataFrame(data)

def get_stock_info(symbol):
    ws = get_sheet("stocks")
    data = ws.get_all_records()
    for row in data:
        if row["symbol"] == symbol.upper():
            return row
    return None

# ── TRADES ─────────────────────────────────────────────
def log_trade(symbol, trade_type, price, quantity, date, notes="", emotion="", mistake=""):
    ws = get_sheet("trades")
    if ws.cell(1,1).value != "symbol":
        ws.append_row(["symbol","trade_type","price","quantity","date","notes","emotion","mistake","created_at"])
    ws.append_row([symbol.upper(), trade_type.upper(), price, quantity, date, notes, emotion, mistake, str(datetime.now())])
    return f"Trade logged for {symbol}."

def get_stock_history(symbol):
    ws = get_sheet("trades")
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    if symbol == "":
        return df
    return df[df["symbol"] == symbol.upper()].sort_values("date", ascending=False)

def calculate_pnl(symbol):
    df = get_stock_history(symbol)
    if df.empty:
        return None
    buys = df[df["trade_type"] == "BUY"]
    sells = df[df["trade_type"] == "SELL"]
    total_invested = (buys["price"] * buys["quantity"]).sum()
    total_returned = (sells["price"] * sells["quantity"]).sum()
    pnl = total_returned - total_invested
    return {
        "invested": total_invested,
        "returned": total_returned,
        "pnl": pnl,
        "pnl_pct": (pnl / total_invested * 100) if total_invested > 0 else 0
    }

# ── WATCHLIST ──────────────────────────────────────────
def add_to_watchlist(symbol, target, stop_loss, reason=""):
    ws = get_sheet("watchlist")
    if ws.cell(1,1).value != "symbol":
        ws.append_row(["symbol","target_price","stop_loss","reason","added_at"])
    ws.append_row([symbol.upper(), target, stop_loss, reason, str(datetime.now())])
    return f"{symbol} added to watchlist."

def get_watchlist():
    ws = get_sheet("watchlist")
    data = ws.get_all_records()
    return pd.DataFrame(data)

import yfinance as yf

# ── ALERTS ─────────────────────────────────────────────
def add_alert(symbol, price, direction, chat_id):
    ws = get_sheet("alerts")
    if ws.cell(1,1).value != "symbol":
        ws.append_row(["symbol", "price", "direction", "chat_id", "triggered", "added_at"])
    ws.append_row([symbol.upper(), price, direction, str(chat_id), "NO", str(datetime.now())])
    return f"Alert added for {symbol}."

def get_active_alerts():
    ws = get_sheet("alerts")
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    return df[df["triggered"] == "NO"]

def mark_alert_triggered(symbol, price, direction):
    ws = get_sheet("alerts")
    data = ws.get_all_records()
    for i, row in enumerate(data):
        if (row["symbol"] == symbol.upper() and
            float(row["price"]) == price and
            row["direction"] == direction and
            row["triggered"] == "NO"):
            ws.update_cell(i + 2, 5, "YES")  # column 5 = triggered
            break

# ── LIVE PRICE ─────────────────────────────────────────
def get_live_price(symbol):
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        data = ticker.history(period="1d", interval="1m")
        if not data.empty:
            return round(data["Close"].iloc[-1], 2)
        return None
    except:
        return None
    
def get_nifty500_symbols():
    ws = get_sheet("nifty 500")
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        return []
    # column header name — check what row 1 says in your sheet
    col = df.columns[2]  # column C (index 2)
    symbols = df[col].dropna().str.strip().str.upper().tolist()
    return [s for s in symbols if s]