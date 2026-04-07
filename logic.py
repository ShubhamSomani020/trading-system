import sqlite3
import pandas as pd
from database import get_connection

def add_stock(symbol, name="", sector="", support=0, resistance=0, notes=""):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO stocks (symbol, name, sector, support_zone, resistance_zone, study_notes)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (symbol.upper(), name, sector, support, resistance, notes))
        conn.commit()
        return f"{symbol} added successfully."
    except sqlite3.IntegrityError:
        return f"{symbol} already exists."
    finally:
        conn.close()

def log_trade(symbol, trade_type, price, quantity, date, notes="", emotion="", mistake=""):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO trades (symbol, trade_type, price, quantity, date, notes, emotion, mistake)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (symbol.upper(), trade_type.upper(), price, quantity, date, notes, emotion, mistake))
    conn.commit()
    conn.close()
    return f"Trade logged for {symbol}."

def get_stock_history(symbol):
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM trades WHERE symbol=? ORDER BY date DESC",
        conn, params=(symbol.upper(),)
    )
    conn.close()
    return df

def get_stock_info(symbol):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM stocks WHERE symbol=?", (symbol.upper(),))
    row = c.fetchone()
    conn.close()
    return row

def calculate_pnl(symbol):
    df = get_stock_history(symbol)
    if df.empty:
        return None
    buys = df[df['trade_type'] == 'BUY']
    sells = df[df['trade_type'] == 'SELL']
    total_invested = (buys['price'] * buys['quantity']).sum()
    total_returned = (sells['price'] * sells['quantity']).sum()
    pnl = total_returned - total_invested
    return {
        "invested": total_invested,
        "returned": total_returned,
        "pnl": pnl,
        "pnl_pct": (pnl / total_invested * 100) if total_invested > 0 else 0
    }

def add_to_watchlist(symbol, target, stop_loss, reason=""):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO watchlist (symbol, target_price, stop_loss, reason)
        VALUES (?, ?, ?, ?)
    ''', (symbol.upper(), target, stop_loss, reason))
    conn.commit()
    conn.close()
    return f"{symbol} added to watchlist."

def get_watchlist():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM watchlist ORDER BY added_at DESC", conn)
    conn.close()
    return df

def get_all_stocks():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM stocks ORDER BY symbol", conn)
    conn.close()
    return df