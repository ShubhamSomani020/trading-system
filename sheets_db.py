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
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDENTIALS"))
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