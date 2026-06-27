#!/usr/bin/env python3
# PolySniper LIVE v4.0 - AUTO-TRADING + STOP LOSS
# Bot uses Polymarket's Chainlink WebSocket feed for prices.
# Now with AUTO-TRADING enabled (assumes API bug fixed via Phantom wallet setup).
# Stop loss at 50¢: if share value drops below 50¢, bot sells to cut losses.
# Auto-pause after 2 consecutive losses to protect against bad streaks.

import os
import time
import sqlite3
import logging
import requests
import json
import threading
from datetime import datetime, timezone, timedelta
from collections import deque

try:
    import websocket  # websocket-client library
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, PartialCreateOrderOptions,
        BalanceAllowanceParams, AssetType, OrderType
    )
    from py_clob_client_v2.order_builder.constants import BUY
    V2_AVAILABLE = True
    IMPORT_ERROR = None
except ImportError as e:
    V2_AVAILABLE = False
    IMPORT_ERROR = str(e)

# ─── ENV VARS ─────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "").strip()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
TIMEFRAMES = [5, 15]

ASSET_THRESHOLDS_5 = {
    "BTC":  float(os.environ.get("THRESHOLD_BTC_5",  "0.09")),
    "ETH":  float(os.environ.get("THRESHOLD_ETH_5",  "0.10")),
    "SOL":  float(os.environ.get("THRESHOLD_SOL_5",  "0.15")),
    "DOGE": float(os.environ.get("THRESHOLD_DOGE_5", "0.20")),
    "BNB":  float(os.environ.get("THRESHOLD_BNB_5",  "0.12")),
    "XRP":  float(os.environ.get("THRESHOLD_XRP_5",  "0.15")),
    "HYPE": float(os.environ.get("THRESHOLD_HYPE_5", "0.20")),
}
ASSET_THRESHOLDS_15 = {
    "BTC":  float(os.environ.get("THRESHOLD_BTC_15", "0.15")),
    "ETH":  float(os.environ.get("THRESHOLD_ETH_15", "0.20")),
    "SOL":  float(os.environ.get("THRESHOLD_SOL_15", "0.30")),
    "DOGE": float(os.environ.get("THRESHOLD_DOGE_15", "0.40")),
    "BNB":  float(os.environ.get("THRESHOLD_BNB_15", "0.25")),
    "XRP":  float(os.environ.get("THRESHOLD_XRP_15", "0.30")),
    "HYPE": float(os.environ.get("THRESHOLD_HYPE_15", "0.40")),
}

MAX_REVERSALS_5  = int(os.environ.get("MAX_REVERSALS_5",  "35"))
MAX_REVERSALS_15 = int(os.environ.get("MAX_REVERSALS_15", "50"))

# Volatility filter (separate from reversals): measures average per-second % move
# over the last 30s — how "jumpy" the market is, regardless of direction.
# MAX_VOLATILITY_PCT = 0 means MEASURE-ONLY (logs the number, never skips).
# Set it to a value (e.g. 0.015) later to start skipping windows jumpier than that.
# The volatility is always logged so you can collect real numbers before choosing.
MAX_VOLATILITY_PCT = float(os.environ.get("MAX_VOLATILITY_PCT", "0.0"))

CONFIG = {
    "bet_size":             float(os.environ.get("BET_SIZE", "5.0")),
    # Time-based bet sizing (EST). Day = 6am-9pm, Night = 9pm-6am.
    # If TIME_BASED_BET is "true", these override bet_size based on the hour.
    "time_based_bet":       os.environ.get("TIME_BASED_BET", "false").lower() == "true",
    "bet_size_day":         float(os.environ.get("BET_SIZE_DAY", "5.0")),
    "bet_size_night":       float(os.environ.get("BET_SIZE_NIGHT", "5.0")),
    "night_start_hour":     int(os.environ.get("NIGHT_START_HOUR", "21")),  # 9pm EST
    "night_end_hour":       int(os.environ.get("NIGHT_END_HOUR", "6")),     # 6am EST
    "entry_window_seconds": int(os.environ.get("ENTRY_SECS", "50")),
    "retry_interval_secs":  int(os.environ.get("RETRY_INTERVAL", "5")),
    "momentum_checks":      int(os.environ.get("MOMENTUM_CHECKS", "1")),
    "price_agreement_pct":  float(os.environ.get("PRICE_AGREEMENT_PCT", "0.20")),
    "price_interval_secs":  1,
    "min_balance":          float(os.environ.get("MIN_BALANCE", "1.0")),
    "min_entry_cents":      float(os.environ.get("MIN_ENTRY_CENTS", "95.0")),
    "max_entry_cents":      float(os.environ.get("MAX_ENTRY_CENTS", "99.9")),
    "choppy_threshold":     int(os.environ.get("CHOPPY_THRESHOLD", "20")),
    "stop_loss_cents":      float(os.environ.get("STOP_LOSS_CENTS", "50.0")),
    "stop_loss_check_secs": int(os.environ.get("STOP_LOSS_CHECK_SECS", "10")),
    "stop_loss_discount":   float(os.environ.get("STOP_LOSS_DISCOUNT", "0.05")),
    "consecutive_loss_limit": int(os.environ.get("CONSECUTIVE_LOSS_LIMIT", "2")),
    # One-asset-at-a-time: if true, block entering a new asset while a DIFFERENT
    # asset has an open position. Same asset on 5m + 15m together is still allowed.
    "one_asset_at_a_time": os.environ.get("ONE_ASSET_AT_A_TIME", "true").lower() == "true",
}

ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡", "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}

# Polymarket event-slug short names. Mostly the lowercase ticker, but HYPE's slug
# uses "hype" (the market is titled "Hyperliquid" but the slug is hype-updown-...).
ASSET_SLUG = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge",
              "BNB": "bnb", "XRP": "xrp", "HYPE": "hype"}

# Per-asset entry-price range overrides (cents). If an asset is listed here, its
# min/max override the global CONFIG min_entry_cents/max_entry_cents.
# SOL: 97-100¢ (set per request). Others fall back to the global range.
ASSET_ENTRY_RANGE = {
    "SOL": (
        float(os.environ.get("MIN_ENTRY_CENTS_SOL", "97.0")),
        float(os.environ.get("MAX_ENTRY_CENTS_SOL", "100.0")),
    ),
}

def entry_range_for(asset):
    """Return (min_cents, max_cents) for an asset, using per-asset override if set."""
    if asset in ASSET_ENTRY_RANGE:
        return ASSET_ENTRY_RANGE[asset]
    return CONFIG["min_entry_cents"], CONFIG["max_entry_cents"]


COINBASE_PRODUCTS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "DOGE": "DOGE-USD", "BNB": "BNB-USD"}
COINGECKO_IDS     = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "DOGE": "dogecoin", "BNB": "binancecoin"}

# ─── STATE ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("polysniper-live")

state = {
    "balance_usd":  0.0,
    "paused":       False,
    "pause_reason": None,
    "trades_today": 0,
    "pnl_today":    0.0,
    "wins_today":   0,
    "losses_today": 0,
    "consecutive_losses": 0,
    "last_balance_check": 0,
    "connected":    False,
    "last_msg_time": time.time(),
}

# Track open positions for stop loss monitoring
# Format: {trade_db_id: {token_id, shares, entry_cents, asset, tf, direction, ...}}
open_positions = {}

windows           = {}
prices            = {}
prices_cc         = {}
prices_cb         = {}
prices_cg         = {}
prices_chainlink  = {}  # From Polymarket's RTDS WebSocket - same source as their settlement
price_histories   = {}
active_directions = {}
for a in ASSET_LIST:
    for tf in TIMEFRAMES:
        price_histories[(a, tf)] = deque(maxlen=180)
last_prices_time = 0
update_offset    = None

clob_client = None
market_cache = {}  # cache of slug → token IDs


# ─── TIME HELPERS ────────────────────────────────────────────────────────────
def est_now():
    # Proper Eastern time (handles daylight saving). Falls back to fixed UTC-4.
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=4)

def current_bet_size():
    """
    Return the bet size for right now. If time-based betting is on, use the
    day size (6am-9pm EST) or night size (9pm-6am EST). Otherwise the flat bet_size.
    """
    if not CONFIG.get("time_based_bet"):
        return CONFIG["bet_size"]
    hour = est_now().hour
    start = CONFIG["night_start_hour"]  # 21 (9pm)
    end = CONFIG["night_end_hour"]      # 6 (6am)
    # Night wraps past midnight: hour >= 21 OR hour < 6
    is_night = (hour >= start) or (hour < end)
    return CONFIG["bet_size_night"] if is_night else CONFIG["bet_size_day"]

def est_str():
    return est_now().strftime("%H:%M EST")

def est_full():
    return est_now().strftime("%b %d %H:%M EST")

def fmt_time_short(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - timedelta(hours=4)).strftime("%H:%M")
    except:
        return "??:??"


# ─── DATABASE ────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entered_at TEXT, asset TEXT, timeframe INTEGER, direction TEXT,
            pct_move REAL, open_price REAL, entry_price REAL,
            real_entry_cents REAL, shares REAL, cost REAL,
            order_id TEXT, close_price REAL, result TEXT,
            payout REAL, profit_loss REAL, balance_after REAL,
            window_open TEXT, window_close TEXT
        )
    """)
    conn.commit()
    conn.close()


def db_insert_trade(t):
    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    c = conn.cursor()
    c.execute("""
        INSERT INTO trades (entered_at, asset, timeframe, direction, pct_move,
            open_price, entry_price, real_entry_cents, shares, cost, order_id,
            window_open, window_close, result)
        VALUES (:entered_at, :asset, :timeframe, :direction, :pct_move,
            :open_price, :entry_price, :real_entry_cents, :shares, :cost, :order_id,
            :window_open, :window_close, 'PENDING')
    """, t)
    row_id = c.lastrowid
    conn.commit()
    conn.close()
    return row_id


def db_log_skip(asset, tf, direction, pct_move, open_price, entry_cents, reason, open_time, close_time, secs_left):
    """Log a skipped signal to the trades DB for history tracking (no money spent)."""
    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    c.execute("""
        INSERT INTO trades (entered_at, asset, timeframe, direction, pct_move,
            open_price, entry_price, real_entry_cents, shares, cost, order_id,
            window_open, window_close, result, profit_loss)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, 'SKIPPED', 0)
    """, (now, asset, tf, direction, round(pct_move, 4),
          open_price, open_price, round(entry_cents, 1),
          reason[:100], open_time.isoformat(), close_time.isoformat()))
    conn.commit()
    conn.close()


def db_settle(row_id, close_price, result, payout, pl, bal):
    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    conn.execute("""
        UPDATE trades SET close_price=?, result=?, payout=?,
        profit_loss=?, balance_after=? WHERE id=?
    """, (close_price, result, payout, pl, bal, row_id))
    conn.commit()
    conn.close()


def db_stats():
    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    c = conn.cursor()
    c.execute("""
        SELECT COUNT(*), SUM(CASE WHEN result='WON' THEN 1 ELSE 0 END), SUM(profit_loss)
        FROM trades WHERE result IN ('WON','LOST')
    """)
    r = c.fetchone()
    conn.close()
    return r[0] or 0, r[1] or 0, r[2] or 0.0


def db_recent_trades(limit=10, offset=0):
    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    c = conn.cursor()
    c.execute("""
        SELECT entered_at, asset, timeframe, direction, pct_move, result, profit_loss, real_entry_cents, window_close
        FROM trades ORDER BY id DESC LIMIT ? OFFSET ?
    """, (limit, offset))
    rows = c.fetchall()
    conn.close()
    return rows


# ─── TELEGRAM ────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        state["last_msg_time"] = time.time()
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8
        )
        log.info(f"[TG] {msg[:80]}")
    except Exception as e:
        log.error(f"TG error: {e}")


def get_updates():
    global update_offset
    try:
        params = {"timeout": 1, "allowed_updates": ["message"]}
        if update_offset:
            params["offset"] = update_offset
        res = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params=params, timeout=5
        )
        return res.json().get("result", [])
    except:
        return []


def handle_commands():
    global update_offset
    for update in get_updates():
        update_offset = update["update_id"] + 1
        msg  = update.get("message", {})
        text = msg.get("text", "").strip().lower()
        cid  = str(msg.get("chat", {}).get("id", ""))
        if cid != str(TELEGRAM_CHAT_ID):
            continue

        if text == "/pause":
            state["paused"] = True
            state["pause_reason"] = "Manual"
            tg("⏸ <b>Paused</b>\n/resume to continue")

        elif text == "/resume":
            state["paused"] = False
            state["pause_reason"] = None
            tg("▶️ <b>Resumed</b>")

        elif text == "/stop":
            state["paused"] = True
            state["pause_reason"] = "Emergency stop"
            tg("🛑 <b>EMERGENCY STOP</b>\n/resume to restart")

        elif text == "/status":
            total, wins, pnl = db_stats()
            wr = f"{wins/total*100:.1f}%" if total > 0 else "—"
            # Count skipped signals
            conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades WHERE result='SKIPPED'")
            skipped = c.fetchone()[0] or 0
            conn.close()
            status_e = "⏸" if state["paused"] else "🟢"
            status_w = "PAUSED" if state["paused"] else "ACTIVE"
            tg(
                f"{status_e} <b>{status_w}</b>\n\n"
                f"💰 Balance: <b>${state['balance_usd']:.2f}</b>\n"
                f"🎯 Bet: ${current_bet_size():g} (day ${CONFIG['bet_size_day']:g} / night ${CONFIG['bet_size_night']:g})\n"
                f"📊 Today: {state['trades_today']} · ${state['pnl_today']:+.2f}\n"
                f"🏆 All-time: {total} · {wr} · ${pnl:+.2f}\n"
                f"⏭ Skipped: {skipped}\n"
                f"🕐 {est_str()}"
            )

        elif text == "/balance":
            bal = check_balance(force=True)
            tg(f"💰 <b>Balance:</b> ${bal:.2f}\n🕐 {est_str()}")

        elif text == "/history" or text.startswith("/history "):
            page = 1
            parts = text.split()
            if len(parts) > 1:
                try: page = max(1, int(parts[1]))
                except: page = 1
            per_page = 10
            offset = (page - 1) * per_page

            conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades")
            total = c.fetchone()[0] or 0
            conn.close()

            trades = db_recent_trades(per_page, offset)
            max_pages = max(1, (total + per_page - 1) // per_page)

            out = f"📜 <b>History · Page {page}/{max_pages}</b>\n\n"
            if not trades:
                out += "(no trades yet)\n"
            else:
                for row in trades:
                    t, a, tf, d, pct, res, pl, ec, wc = row
                    emoji = ASSET_EMOJI.get(a, "")
                    arrow = "🔺" if d == "UP" else "🔻"
                    if res == "WON":
                        res_str = "✅"
                    elif res == "LOST":
                        res_str = "❌"
                    elif res == "SKIPPED":
                        res_str = "⏭"
                    else:
                        res_str = "⏳"
                    pl_str = f"${pl:+.2f}" if pl is not None and res != "SKIPPED" else ("—" if res == "SKIPPED" else "pending")
                    # Calculate seconds left when entered
                    secs_left_str = ""
                    try:
                        entered_dt = datetime.fromisoformat(t)
                        close_dt = datetime.fromisoformat(wc)
                        if entered_dt.tzinfo is None: entered_dt = entered_dt.replace(tzinfo=timezone.utc)
                        if close_dt.tzinfo is None: close_dt = close_dt.replace(tzinfo=timezone.utc)
                        secs_left = max(0, int((close_dt - entered_dt).total_seconds()))
                        secs_left_str = f" ({secs_left}s)"
                    except:
                        pass
                    out += f"  {fmt_time_short(t)}{secs_left_str} {emoji}{a} {tf}m {arrow} {pct:+.2f}% @{ec or '?'}¢ {res_str} {pl_str}\n"
            if page < max_pages:
                out += f"\n➡️ /history {page + 1}"
            if page > 1:
                out += f"\n⬅️ /history {page - 1}"
            tg(out)

        elif text == "/help":
            tg(
                "🤖 <b>LIVE Bot Commands</b>\n\n"
                "/status — quick status\n"
                "/balance — refresh real balance\n"
                "/history — recent trades\n"
                "/pause — pause trading\n"
                "/resume — resume\n"
                "/stop — emergency stop"
            )


# ─── POLYMARKET CONNECTION ───────────────────────────────────────────────────
def init_clob():
    global clob_client
    if not V2_AVAILABLE:
        tg(f"❌ <b>py-clob-client-v2 import failed</b>\n<code>{IMPORT_ERROR}</code>")
        return False
    if not POLY_PRIVATE_KEY or not POLY_FUNDER:
        tg("❌ <b>POLY_PRIVATE_KEY or POLY_FUNDER missing</b>")
        return False

    host = "https://clob.polymarket.com"
    chain_id = 137

    try:
        log.info("Creating temporary client for L1 auth (with sig=3)...")
        temp = ClobClient(
            host=host,
            chain_id=chain_id,
            key=POLY_PRIVATE_KEY,
            signature_type=3,
            funder=POLY_FUNDER,
        )
        creds = temp.create_or_derive_api_key()

        log.info("Creating authenticated client (sig=3, POLY_1271)...")
        clob_client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=POLY_PRIVATE_KEY,
            creds=creds,
            signature_type=3,
            funder=POLY_FUNDER,
        )
        check_balance(force=True)
        state["connected"] = True
        log.info(f"Connected. Balance: ${state['balance_usd']:.2f}")
        return True
    except Exception as e:
        log.error(f"CLOB init failed: {e}")
        tg(f"❌ <b>Polymarket connection failed</b>\n<code>{str(e)[:200]}</code>")
        return False


def check_balance(force=False):
    global clob_client
    now = time.time()
    if not force and now - state["last_balance_check"] < 60:
        return state["balance_usd"]
    if not clob_client:
        return 0.0
    try:
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        bal_resp = clob_client.get_balance_allowance(params)
        if isinstance(bal_resp, dict):
            bal_raw = bal_resp.get("balance", 0)
        else:
            bal_raw = getattr(bal_resp, "balance", 0)
        state["balance_usd"] = float(bal_raw) / 1_000_000
        state["last_balance_check"] = now
        return state["balance_usd"]
    except Exception as e:
        log.error(f"Balance check failed: {e}")
        return state["balance_usd"]


# ─── PRICE FEEDS ─────────────────────────────────────────────────────────────
def fetch_cryptocompare():
    return {}  # Disabled in Chainlink-only mode


def fetch_coinbase():
    return {}  # Disabled in Chainlink-only mode


def fetch_coingecko():
    return {}  # Disabled in Chainlink-only mode


# ─── CHAINLINK WEBSOCKET (Polymarket's actual settlement source) ─────────────
CHAINLINK_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOLS = {
    "BTC":  "btc/usd",
    "ETH":  "eth/usd",
    "SOL":  "sol/usd",
    "DOGE": "doge/usd",
    "BNB":  "bnb/usd",
    "XRP":  "xrp/usd",
    "HYPE": "hype/usd",
}
chainlink_last_update = {}  # Track when we last got a price for each asset


def _extract_latest_value(payload):
    """Pull the newest price value out of a Chainlink payload (snapshot or single tick)."""
    if not isinstance(payload, dict):
        return None
    data_arr = payload.get("data")
    if isinstance(data_arr, list) and data_arr:
        latest = data_arr[-1]
        if isinstance(latest, dict):
            v = float(latest.get("value", 0))
            return v if v > 0 else None
    if "value" in payload:
        v = float(payload.get("value", 0))
        return v if v > 0 else None
    return None


def chainlink_websocket_worker(asset):
    """
    FAST SNAPSHOT MODE: Polymarket's RTDS Chainlink feed is snapshot-on-subscribe —
    it sends one snapshot of recent ticks when you connect, then goes silent. Holding
    the connection open gets you nothing further. So to get fresh data we connect,
    grab the snapshot's newest tick, close, and reconnect on a short cycle.

    RECONNECT_INTERVAL controls how fresh the price is (lower = fresher but more
    connection load; too low risks rate-limiting/throttling by Polymarket).
    """
    symbol = CHAINLINK_SYMBOLS.get(asset)
    if not symbol:
        return

    RECONNECT_INTERVAL = float(os.environ.get("CL_RECONNECT_SECS", "1.0"))

    while True:
        ws = None
        try:
            ws = websocket.create_connection(CHAINLINK_WS_URL, timeout=10)
            ws.settimeout(5)

            subscribe_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": json.dumps({"symbol": symbol})
                    }
                ]
            }
            ws.send(json.dumps(subscribe_msg))

            # Read until we get the snapshot's price, then move on (up to 5s).
            connect_time = time.time()
            got_data = False
            while time.time() - connect_time < 5:
                try:
                    msg = ws.recv()
                    if not msg:
                        continue
                    data = json.loads(msg)
                    value = _extract_latest_value(data.get("payload", {}))
                    if value is not None:
                        prices_chainlink[asset] = value
                        chainlink_last_update[asset] = time.time()
                        got_data = True
                        break
                except websocket.WebSocketTimeoutException:
                    break
                except Exception as e:
                    log.warning(f"[Chainlink WS] {asset} recv error: {e}")
                    break

            if not got_data:
                log.warning(f"[Chainlink WS] {asset} no data in snapshot")

        except Exception as e:
            log.error(f"[Chainlink WS] {asset} connection error: {e}")
        finally:
            try:
                if ws:
                    ws.close()
            except:
                pass

        # Short cycle for fresher data. Watch logs for rate-limit/connection errors;
        # if they appear, raise CL_RECONNECT_SECS (e.g. to 2 or 3).
        time.sleep(RECONNECT_INTERVAL)


def start_chainlink_threads():
    """Start a background thread for each asset to maintain Chainlink WS connection."""
    if not WEBSOCKET_AVAILABLE:
        log.warning("websocket-client not installed - Chainlink feed disabled")
        return False
    for asset in ASSET_LIST:
        t = threading.Thread(target=chainlink_websocket_worker, args=(asset,), daemon=True)
        t.start()
    log.info(f"[Chainlink WS] Started {len(ASSET_LIST)} background threads")
    return True


def get_chainlink_price(asset):
    """
    Get the latest Chainlink price for an asset.
    Returns None if no recent update (data older than 30 seconds is stale).
    """
    if asset not in prices_chainlink:
        return None
    last_update = chainlink_last_update.get(asset, 0)
    if time.time() - last_update > 30:
        return None  # Stale data, don't use
    return prices_chainlink[asset]


def fetch_validated_prices():
    """
    CHAINLINK-ONLY MODE: Returns prices from Polymarket's Chainlink WebSocket feed ONLY.
    This is the same source Polymarket uses for settlement, ensuring no data mismatch.

    If Chainlink is unavailable (>30s stale), the asset is skipped entirely.
    No fallback to CryptoCompare/Coinbase/CoinGecko.
    """
    validated = {}
    for asset in ASSET_LIST:
        chainlink_price = get_chainlink_price(asset)
        if chainlink_price:
            validated[asset] = chainlink_price
        # No fallback - if Chainlink doesn't have data, asset is skipped
    return validated


# ─── BINANCE LAG MEASUREMENT (measure-only — no trading impact) ──────────────
# Streams live prices from Binance (a true push stream) and measures how long
# Polymarket's Chainlink relay takes to reflect meaningful moves. Pure
# instrumentation: no extra Polymarket API calls, separate daemon threads,
# cannot affect trading. Per-market hourly summary to Telegram.
# Toggle with LAG_MEASURE=false.
LAG_MEASURE        = os.environ.get("LAG_MEASURE", "true").lower() == "true"
LAG_EVENT_PCT      = float(os.environ.get("LAG_EVENT_PCT", "0.05"))    # min Binance move (%) to count as an event
LAG_EVENT_WINDOW   = float(os.environ.get("LAG_EVENT_WINDOW", "5"))    # the move must happen within this many seconds
LAG_EVENT_COOLDOWN = float(os.environ.get("LAG_EVENT_COOLDOWN", "60")) # per-asset gap between events (s)
LAG_MATCH_FRACTION = float(os.environ.get("LAG_MATCH_FRACTION", "0.5"))# Poly feed must show this fraction of the move
LAG_MAX_WAIT       = float(os.environ.get("LAG_MAX_WAIT", "30"))       # give up waiting after this many seconds
LAG_TG_PER_EVENT   = os.environ.get("LAG_TG_PER_EVENT", "false").lower() == "true"
LAG_REPORT_SECS    = int(os.environ.get("LAG_REPORT_SECS", "3600"))    # hourly summary

BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@trade" for s in ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "bnbusdt", "xrpusdt"]
)
# HYPE is handled separately via Binance FUTURES below (not on spot).
BINANCE_SYMBOL_TO_ASSET = {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL",
                           "DOGEUSDT": "DOGE", "BNBUSDT": "BNB", "XRPUSDT": "XRP"}

# HYPE is not on Binance SPOT, but it IS on Binance USD-M FUTURES (HYPEUSDT perp).
# Futures use a different host (fstream) so they need a separate connection. The
# @trade payload shape is identical to spot, so it feeds the same _lag_ticks.
# Caveat: futures price can carry a small funding/basis vs Chainlink HYPE/USD, so
# HYPE's reversal signal is slightly noisier than the spot-based assets - still
# far better than no protection.
# Endpoint note: Binance decommissioned the legacy futures WS URLs
# (fstream.binance.com/stream and /ws) on 2026-04-23. Post-migration, public
# market streams like @trade must use the /public routed path.
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/public/stream?streams=" + "/".join(
    f"{s}@trade" for s in ["hypeusdt"]
)
BINANCE_FUTURES_SYMBOL_TO_ASSET = {"HYPEUSDT": "HYPE"}

prices_binance      = {}
binance_last_update = {}
_lag_ticks      = {a: deque() for a in ASSET_LIST}   # recent (ts, price) Binance ticks
_lag_last_event = {a: 0.0 for a in ASSET_LIST}
_lag_results    = {a: [] for a in ASSET_LIST}        # lag seconds, current report window
_lag_timeouts   = {a: 0 for a in ASSET_LIST}         # events the feed never matched in time
_lag_lock       = threading.Lock()


def _lag_on_tick(asset, ts, price):
    """Called on every Binance tick: detect 'events' (fast meaningful moves)."""
    dq = _lag_ticks[asset]
    dq.append((ts, price))
    cutoff = ts - LAG_EVENT_WINDOW
    while dq and dq[0][0] < cutoff:
        dq.popleft()
    if len(dq) < 2:
        return
    base_ts, base_price = dq[0]
    if base_price <= 0:
        return
    move_pct = (price - base_price) / base_price * 100
    if abs(move_pct) < LAG_EVENT_PCT:
        return
    if ts - _lag_last_event[asset] < LAG_EVENT_COOLDOWN:
        return
    _lag_last_event[asset] = ts
    cl_start = prices_chainlink.get(asset)
    if not cl_start:
        return  # can't measure lag without a Poly feed baseline
    threading.Thread(target=_lag_watch, args=(asset, ts, move_pct, cl_start), daemon=True).start()


def _lag_watch(asset, event_ts, move_pct, cl_start):
    """Wait for Polymarket's Chainlink relay to reflect the Binance move; record the lag."""
    try:
        needed = abs(move_pct) * LAG_MATCH_FRACTION
        sign = 1 if move_pct > 0 else -1
        while time.time() - event_ts < LAG_MAX_WAIT:
            cl_now = prices_chainlink.get(asset)
            if cl_now and cl_start > 0:
                cl_move = (cl_now - cl_start) / cl_start * 100
                if cl_move * sign >= needed:
                    lag = time.time() - event_ts
                    with _lag_lock:
                        _lag_results[asset].append(lag)
                    log.info(f"[LAG] {asset} Binance {move_pct:+.3f}% -> Poly feed caught up in {lag:.1f}s")
                    if LAG_TG_PER_EVENT:
                        tg(
                            f"⚡ <b>LAG · {ASSET_EMOJI.get(asset,'')} {asset}</b>\n"
                            f"Binance move: {move_pct:+.3f}%\n"
                            f"Poly feed caught up in: <b>{lag:.1f}s</b>"
                        )
                    return
            time.sleep(0.2)
        with _lag_lock:
            _lag_timeouts[asset] += 1
        log.info(f"[LAG] {asset} Binance {move_pct:+.3f}% -> feed did NOT catch up within {LAG_MAX_WAIT:.0f}s")
    except Exception as e:
        log.warning(f"[LAG] watch error for {asset}: {e}")


def binance_ws_worker():
    """Persistent Binance stream — a true push feed (stays open, sends every trade)."""
    while True:
        ws = None
        try:
            ws = websocket.create_connection(BINANCE_WS_URL, timeout=10)
            ws.settimeout(30)
            log.info("[Binance WS] Connected (lag measurement)")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                data = json.loads(msg)
                d = data.get("data", {})
                asset = BINANCE_SYMBOL_TO_ASSET.get(d.get("s", ""))
                if not asset:
                    continue
                price = float(d.get("p", 0))
                if price <= 0:
                    continue
                now = time.time()
                prices_binance[asset] = price
                binance_last_update[asset] = now
                _lag_on_tick(asset, now, price)
        except Exception as e:
            log.warning(f"[Binance WS] error: {e} - reconnecting in 3s")
        finally:
            try:
                if ws:
                    ws.close()
            except:
                pass
        time.sleep(3)


def binance_futures_ws_worker():
    """Persistent Binance USD-M FUTURES stream for HYPE (not on spot). Feeds the
    same _lag_ticks / binance_last_update as the spot worker, so HYPE gets the
    same reversal-cancel protection as every other asset."""
    while True:
        ws = None
        try:
            ws = websocket.create_connection(BINANCE_FUTURES_WS_URL, timeout=10)
            ws.settimeout(30)
            log.info("[Binance Futures WS] Connected (HYPE)")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                data = json.loads(msg)
                d = data.get("data", {})
                asset = BINANCE_FUTURES_SYMBOL_TO_ASSET.get(d.get("s", ""))
                if not asset:
                    continue
                price = float(d.get("p", 0))
                if price <= 0:
                    continue
                now = time.time()
                prices_binance[asset] = price
                binance_last_update[asset] = now
                _lag_on_tick(asset, now, price)
        except Exception as e:
            log.warning(f"[Binance Futures WS] error: {e} - reconnecting in 3s")
        finally:
            try:
                if ws:
                    ws.close()
            except:
                pass
        time.sleep(3)


def lag_report_worker():
    """Hourly per-market lag summary to Telegram."""
    import statistics
    while True:
        time.sleep(LAG_REPORT_SECS)
        try:
            with _lag_lock:
                snapshot = {a: list(_lag_results[a]) for a in ASSET_LIST}
                touts = dict(_lag_timeouts)
                for a in ASSET_LIST:
                    _lag_results[a].clear()
                    _lag_timeouts[a] = 0
            total = sum(len(v) for v in snapshot.values()) + sum(touts.values())
            if total == 0:
                continue  # quiet hour - no message
            lines = []
            for a in ASSET_LIST:
                vals = snapshot[a]
                t_o = touts.get(a, 0)
                n = len(vals) + t_o
                em = ASSET_EMOJI.get(a, "")
                if n == 0:
                    lines.append(f"{em} {a} · no events")
                elif vals:
                    med = statistics.median(vals)
                    over2 = sum(1 for v in vals if v > 2) / len(vals) * 100
                    lines.append(f"{em} {a} · {n} ev · med {med:.1f}s · >2s: {over2:.0f}% · miss: {t_o}")
                else:
                    lines.append(f"{em} {a} · {n} ev · none caught up (>{LAG_MAX_WAIT:.0f}s)")
            tg(
                "📊 <b>LAG REPORT · last hour</b>\n"
                "<i>Binance → Polymarket feed</i>\n\n"
                + "\n".join(lines)
                + f"\n\n⏱ {total} events total\n🕐 {est_str()}"
            )
        except Exception as e:
            log.error(f"Lag report error: {e}")


_binance_thread_started = False

def start_binance_feed():
    """Start the shared Binance stream once (used by lag measurement AND maker mode)."""
    global _binance_thread_started
    if _binance_thread_started:
        return True
    if not WEBSOCKET_AVAILABLE:
        log.warning("[Binance WS] websocket-client missing - feed disabled")
        return False
    threading.Thread(target=binance_ws_worker, daemon=True).start()
    threading.Thread(target=binance_futures_ws_worker, daemon=True).start()
    _binance_thread_started = True
    return True


def start_lag_measurement():
    if not LAG_MEASURE:
        log.info("[LAG] disabled (LAG_MEASURE=false)")
        return False
    if not start_binance_feed():
        return False
    threading.Thread(target=lag_report_worker, daemon=True).start()
    log.info("[LAG] Binance lag measurement started (hourly Telegram reports)")
    return True


# ─── MAKER MODE: rest 99¢ limit bids instead of taking the ask ────────────────
# WHY: taker FAK orders need shares on the ask side, which is exactly what's
# missing at the right moments ("no match" / empty book at 100¢). The profitable
# wallet does the opposite: it RESTS a limit BUY at 99¢ and lets sellers come to
# it - it never races the fast bots and never needs the ask side at all.
# PROTECTION is not entry-time statistics; it's the live Binance feed:
#   - the resting bid is cancelled within ~1s if Binance turns against the bet
#   - any unfilled remainder is cancelled just before the window closes
#   - no bid is ever placed (and any resting bid is pulled) if Binance goes dark,
#     because a blind resting order is pure adverse-selection bait.
# Toggle with MAKER_MODE=false to return to the old taker behavior.
MAKER_MODE               = os.environ.get("MAKER_MODE", "true").lower() == "true"
MAKER_BID_CENTS          = float(os.environ.get("MAKER_BID_CENTS", "99.0"))
MAKER_CANCEL_REV_PCT     = float(os.environ.get("MAKER_CANCEL_REV_PCT", "0.02"))   # Binance move against bet (~5s window) that cancels
MAKER_CANCEL_T_SECS      = float(os.environ.get("MAKER_CANCEL_T_SECS", "1.0"))     # cancel unfilled remainder this close to window end
MAKER_FILL_POLL_SECS     = float(os.environ.get("MAKER_FILL_POLL_SECS", "1.0"))
MAKER_BINANCE_FRESH_SECS = float(os.environ.get("MAKER_BINANCE_FRESH_SECS", "10.0"))
# Polymarket platform rule: GTC/GTD limit orders require a minimum of 5 shares
# (≈$4.95 at a 99¢ bid). Bets below that are rejected with a 400 error.
MAKER_MIN_SHARES         = float(os.environ.get("MAKER_MIN_SHARES", "5"))
# Ask floor: if the best ask is below this, the market hasn't converged (it
# disagrees with the signal) and a 99¢ limit would instantly TAKE at that low
# price - a different, riskier bet than the maker model. Mirrors the old
# "market disagrees" gate from taker mode.
MAKER_MIN_ASK_CENTS      = float(os.environ.get("MAKER_MIN_ASK_CENTS", "96.0"))

# ── VARIANT: fill-price bail-out floor ──────────────────────────────────────
# If a resting bid starts getting filled BELOW this price, the outcome is
# collapsing and our bid is absorbing the cascade (this is what produced the
# 49¢ HYPE fill). Cancel the remainder immediately the moment a cheap fill is
# detected. This is separate from MAKER_MIN_ASK_CENTS (which only guards the
# placement instant); this guards the RESTING period.
MAKER_BAILOUT_CENTS      = float(os.environ.get("MAKER_BAILOUT_CENTS", "96.0"))

# ── VARIANT: PER-ASSET 5m frontier — MEASURED from the probe /grid ───────────
# Built from 682k+ settled windows (all 7 assets, both timeframes). The bands
# below use the time-left ranges that had real sample counts (hundreds-to-
# thousands per cell). Three tiers fall out of the data:
#   calm    (BTC/XRP/DOGE/BNB): 0.05 → 0.10 → 0.10 → 0.20 → 0.40
#   mid     (ETH):              0.05 → 0.10 → 0.20 → 0.40 → 0.40
#   volatile(SOL/HYPE):         0.10 → 0.20 → 0.40 → 0.40 → 0.40
# Universal truth across every coin: 70-120s out needs ~0.40% to hold >=99%.
# The 0-10s buzzer band is included now (the big sample finally made it solid).
#
# Each row: (max_secs_left, min_abs_move_pct), ascending max_secs.
PER_ASSET_FRONTIER = {
    "BTC":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.20), (120, 0.40)],
    "ETH":  [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
    "SOL":  [(10, 0.10), (20, 0.20), (40, 0.40), (70, 0.40), (120, 0.40)],
    "XRP":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.20), (120, 0.40)],
    "DOGE": [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.20), (120, 0.40)],
    "BNB":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.20), (120, 0.40)],
    "HYPE": [(10, 0.10), (20, 0.20), (40, 0.40), (70, 0.40), (120, 0.40)],
}

# ── VARIANT: PER-ASSET 15m frontier — MEASURED from the probe /grid ──────────
# Now measured (replaces the old scaling GUESS). A 15m window has far more
# elapsed time, so at long time-left a smaller current move is already "locked"
# than the same time-left in a 5m window — which is why several 15m long-band
# thresholds sit BELOW their 5m counterparts. Some 15m cells were still noisy
# (non-monotonic), so these are smoothed to be monotonic-ascending and rounded
# to bucket edges. Less battle-tested than the 5m table; revisit as 15m fills.
PER_ASSET_FRONTIER_15M = {
    "BTC":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.10), (120, 0.10)],
    "ETH":  [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
    "SOL":  [(10, 0.10), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
    "XRP":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.10), (120, 0.20)],
    "DOGE": [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.20), (120, 0.20)],
    "BNB":  [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.20), (120, 0.20)],
    "HYPE": [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
}

# Fallback for anything past 120s left, or an asset not in the tables.
GLOBAL_FRONTIER = [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)]
FRONTIER_FALLBACK_PCT = float(os.environ.get("FRONTIER_FALLBACK_PCT", "0.40"))
# HYPE floor now redundant — HYPE's measured table is already strict. Off by
# default (0.0); set HYPE_MIN_MOVE_PCT>0 to re-impose an extra hard floor.
HYPE_MIN_MOVE_PCT     = float(os.environ.get("HYPE_MIN_MOVE_PCT", "0.0"))


def _frontier_lookup(bands, secs_left):
    for max_secs, min_move in bands:
        if secs_left <= max_secs:
            return min_move
    return FRONTIER_FALLBACK_PCT

# ── VARIANT: stacking ────────────────────────────────────────────────────────
# Re-enter the SAME window multiple times while the move stays above threshold
# for the time remaining (mirrors the wallet's avg 3.2 fills/window). Each clip
# is a fresh resting bid. Capped to bound exposure: STACK_MAX clips x $bet.
STACK_MAX        = int(os.environ.get("STACK_MAX", "3"))
STACK_GAP_SECS   = float(os.environ.get("STACK_GAP_SECS", "12"))


def frontier_locked(asset, abs_move_pct, secs_left, tf=5):
    """True if (move, time-left) clears THIS asset's MEASURED lock frontier for
    this timeframe. Both 5m and 15m now use measured per-asset tables from the
    probe /grid (no more guessing)."""
    if secs_left <= 0:
        return False
    if tf == 15:
        bands = PER_ASSET_FRONTIER_15M.get(asset, GLOBAL_FRONTIER)
    else:
        bands = PER_ASSET_FRONTIER.get(asset, GLOBAL_FRONTIER)
    need = _frontier_lookup(bands, secs_left)
    if asset == "HYPE" and HYPE_MIN_MOVE_PCT > 0:
        need = max(need, HYPE_MIN_MOVE_PCT)
    return abs_move_pct >= need

resting_orders = {}   # order_id -> info
_min_size_warned = False
resting_lock = threading.Lock()


# Assets that have NO Binance feed at all (neither spot nor futures). For these,
# the Binance-based protections can't apply. Currently empty: HYPE is covered by
# the Binance USD-M futures stream (HYPEUSDT perp), so it gets full protection.
NO_BINANCE_ASSETS = set()


def binance_fresh(asset):
    # Assets with no Binance feed are treated as "fresh" so the maker bot will
    # still rest bids for them (they have no Binance signal to wait on).
    if asset in NO_BINANCE_ASSETS:
        return True
    return time.time() - binance_last_update.get(asset, 0) <= MAKER_BINANCE_FRESH_SECS


def binance_recent_move_pct(asset):
    """Signed % move on Binance over the recent tick window (~last 5s). None if no data."""
    dq = _lag_ticks.get(asset)
    if not dq or len(dq) < 2:
        return None
    _, first_p = dq[0]
    _, last_p = dq[-1]
    if first_p <= 0:
        return None
    return (last_p - first_p) / first_p * 100


def cancel_clob_order(order_id):
    # py-clob-client versions differ on the cancel method name/signature.
    # Try the known variants in order until one works.
    last_err = None
    for attempt in (
        lambda: clob_client.cancel(order_id=order_id),
        lambda: clob_client.cancel(order_id),
        lambda: clob_client.cancel_order(order_id),
        lambda: clob_client.cancel_orders([order_id]),
    ):
        try:
            resp = attempt()
            log.info(f"[MAKER] cancel {str(order_id)[:12]}: {str(resp)[:120]}")
            return True
        except TypeError as e:
            last_err = e
            continue
        except AttributeError as e:
            last_err = e
            continue
        except Exception as e:
            log.warning(f"[MAKER] cancel {str(order_id)[:12]} api error: {e}")
            return False
    log.warning(f"[MAKER] cancel failed {str(order_id)[:12]}: no working cancel method ({last_err})")
    return False


def _live_fill_price_cents(info):
    """Real avg fill price (cents) so far for this order's token, from the public
    trades API. Returns None on error (caller then won't bail on missing data)."""
    try:
        r = requests.get(
            "https://data-api.polymarket.com/trades",
            params={"user": POLY_FUNDER, "market": info.get("condition_id", ""),
                    "takerOnly": "false", "limit": 50},
            timeout=6,
        )
        rows = [t for t in r.json() if str(t.get("side", "")).upper() == "BUY"]
        tok = str(info.get("token_id"))
        toks = [t for t in rows if str(t.get("asset", "")) == tok]
        use = toks if toks else rows
        tot_sh = sum(float(t["size"]) for t in use)
        tot_usd = sum(float(t["size"]) * float(t["price"]) for t in use)
        if tot_sh > 0:
            return (tot_usd / tot_sh) * 100
    except Exception as e:
        log.warning(f"[MAKER] live fill-price check failed: {e}")
    return None


def get_order_matched(order_id):
    """Return (matched_shares, status) for an order, or (None, None) on error."""
    try:
        o = clob_client.get_order(order_id)
        if isinstance(o, dict):
            matched = float(o.get("size_matched", o.get("sizeMatched", 0)) or 0)
            status = str(o.get("status", ""))
        else:
            matched = float(getattr(o, "size_matched", 0) or 0)
            status = str(getattr(o, "status", ""))
        return matched, status
    except Exception as e:
        log.warning(f"[MAKER] get_order failed {str(order_id)[:12]}: {e}")
        return None, None


def place_maker_order(asset, tf, direction, bet_size, open_time, close_time, w):
    """Rest a GTC limit BUY at MAKER_BID_CENTS on the signal side. Returns status dict."""
    condition_id, up_token, down_token = find_market_for_window(asset, tf, open_time)
    if not up_token:
        return {"status": "failed", "error": "Market not found"}
    token_id = up_token if direction == "UP" else down_token

    # Never rest a bid we can't watch: a blind resting order is adverse-selection bait.
    if not binance_fresh(asset):
        return {"status": "failed", "error": "Binance feed stale - won't rest a blind bid"}

    # Market-disagrees gate: a 99¢ limit buy CROSSES any asks below it. If the best
    # ask is well under our bid, we'd instantly take a mid-priced position the
    # market doesn't agree with. Skip those - this experiment is the 99¢ model.
    try:
        pr = clob_client.get_price(token_id, side="SELL")
        best_ask = float(pr.get("price", 0)) if isinstance(pr, dict) else float(pr)
        if 0 < best_ask * 100 < MAKER_MIN_ASK_CENTS:
            return {"status": "skipped",
                    "error": f"ask {best_ask*100:.1f}¢ < {MAKER_MIN_ASK_CENTS:.0f}¢ - market disagrees"}
    except Exception as e:
        # FAIL CLOSED: if we can't verify the ask is above the floor, do NOT place.
        # A blind bid can cross a cheap ask (e.g. 91¢) and fill into a flipping
        # outcome - exactly the entry we're trying to prevent.
        log.warning(f"[MAKER] ask check failed ({e}) - skipping (won't place blind below floor)")
        return {"status": "skipped", "error": f"ask check failed ({e}) - won't place blind"}

    import math
    price = round(MAKER_BID_CENTS / 100.0, 2)
    shares = round(bet_size / price, 2)
    if shares < MAKER_MIN_SHARES:
        return {"status": "min_size",
                "error": f"{shares:g} sh < {MAKER_MIN_SHARES:g}-share platform minimum (need bet >= ${MAKER_MIN_SHARES*price:.2f})"}
    if shares <= 0:
        return {"status": "failed", "error": "shares too small"}

    try:
        order_args = OrderArgs(token_id=token_id, price=price, size=shares, side=BUY)
        resp = clob_client.create_and_post_order(
            order_args,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.GTC,
        )
        log.info(f"[MAKER] place response: {str(resp)[:200]}")
        if isinstance(resp, dict):
            success = resp.get("success", False) or resp.get("status") in ("live", "matched")
            order_id = resp.get("orderID") or resp.get("orderId")
        else:
            success = getattr(resp, "success", False)
            order_id = getattr(resp, "orderID", None)
        if not success or not order_id:
            return {"status": "failed", "error": f"not accepted: {str(resp)[:120]}"}

        with resting_lock:
            resting_orders[order_id] = dict(
                order_id=order_id, token_id=token_id, condition_id=condition_id,
                asset=asset, tf=tf,
                direction=direction, shares=shares, price=price,
                open_time=open_time, close_time=close_time, w=w,
                placed_at=time.time(), last_poll=0.0, matched=0.0,
                notified_fill=False, done=False,
            )
        return {"status": "resting", "order_id": order_id, "shares": shares,
                "entry_cents": price * 100}
    except Exception as e:
        log.error(f"[MAKER] place error: {e}")
        return {"status": "failed", "error": str(e)}


def _finalize_maker(info, reason):
    """Cancel remainder, take the final fill count, record the trade if anything filled."""
    if info.get("done"):
        return
    info["done"] = True
    oid = info["order_id"]
    asset, tf, dirn = info["asset"], info["tf"], info["direction"]
    emoji = ASSET_EMOJI.get(asset, "")
    w = info["w"]
    try:
        cancel_clob_order(oid)
        time.sleep(0.5)
        matched, _status = get_order_matched(oid)
        if matched is None:
            matched = info.get("matched", 0.0)

        if matched and matched > 0:
            # Get the REAL average fill price (a 99¢ limit can fill lower if it
            # crossed asks). Public data API; falls back to limit price on error.
            avg_price = info["price"]
            try:
                r = requests.get(
                    "https://data-api.polymarket.com/trades",
                    params={"user": POLY_FUNDER, "market": info.get("condition_id", ""),
                            "takerOnly": "false", "limit": 50},
                    timeout=8,
                )
                fills = [t for t in r.json()
                         if str(t.get("side", "")).upper() == "BUY"]
                tot_sh = sum(float(t["size"]) for t in fills)
                tot_usd = sum(float(t["size"]) * float(t["price"]) for t in fills)
                if tot_sh > 0 and tot_sh >= matched * 0.9:
                    avg_price = tot_usd / tot_sh
            except Exception as e:
                log.warning(f"[MAKER] fill-price lookup failed ({e}) - using limit price")
            cost = round(matched * avg_price, 4)
            now = datetime.now(timezone.utc)
            trade = {
                "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
                "direction": dirn, "pct_move": round(w.get("entry_move") or 0, 4),
                "open_price": w.get("open_price"), "entry_price": w.get("open_price"),
                "real_entry_cents": round(avg_price * 100, 1),
                "shares": matched, "cost": cost, "order_id": oid,
                "window_open": info["open_time"].isoformat(),
                "window_close": info["close_time"].isoformat(),
            }
            db_id = db_insert_trade(trade)
            w["trade_db_id"] = db_id
            state["balance_usd"] -= cost
            state["trades_today"] += 1
            tg(f"🟢 {emoji}{asset} {tf}m {dirn} · {matched:g}sh@{avg_price*100:.1f}¢ · ${cost:.2f}")
            # If the window already rolled over, schedule settlement ourselves
            # (settle() has a guard so it can't run twice for the same window).
            if datetime.now(timezone.utc) >= info["close_time"]:
                settle(w, asset, tf, prices.get(asset) or w.get("open_price"))
        else:
            # Nothing filled - free the one-asset-at-a-time slot.
            if active_directions.get((asset, tf)) == dirn:
                del active_directions[(asset, tf)]
            log.info(f"[MAKER] {asset} {tf}m {dirn} expired unfilled ({reason})")
    except Exception as e:
        log.error(f"[MAKER] finalize error: {e}")
    finally:
        with resting_lock:
            resting_orders.pop(oid, None)


def maker_monitor():
    """Watches all resting bids: cancels on Binance reversal, stale feed, or window
    end; polls fill progress. Runs every 0.5s."""
    while True:
        try:
            time.sleep(0.5)
            if not resting_orders:
                continue
            now_dt = datetime.now(timezone.utc)
            with resting_lock:
                items = list(resting_orders.values())
            for info in items:
                if info.get("done"):
                    continue
                asset, dirn = info["asset"], info["direction"]
                secs_to_close = (info["close_time"] - now_dt).total_seconds()

                if secs_to_close <= MAKER_CANCEL_T_SECS:
                    threading.Thread(target=_finalize_maker, args=(info, "window closing"), daemon=True).start()
                    continue
                if not binance_fresh(asset):
                    threading.Thread(target=_finalize_maker, args=(info, "Binance feed went stale"), daemon=True).start()
                    continue
                mv = binance_recent_move_pct(asset)
                if mv is not None:
                    against = (dirn == "UP" and mv <= -MAKER_CANCEL_REV_PCT) or \
                              (dirn == "DOWN" and mv >= MAKER_CANCEL_REV_PCT)
                    if against:
                        threading.Thread(target=_finalize_maker, args=(info, f"Binance reversal {mv:+.3f}%"), daemon=True).start()
                        continue

                now = time.time()
                if now - info["last_poll"] >= MAKER_FILL_POLL_SECS:
                    info["last_poll"] = now
                    matched, _status = get_order_matched(info["order_id"])
                    if matched is not None:
                        if matched > info["matched"]:
                            info["matched"] = matched
                            # ── VARIANT BAIL-OUT ──
                            # A fill came in. Check what we ACTUALLY paid. If the
                            # average fill price is below the bail-out floor, the
                            # outcome is collapsing and our bid is absorbing the
                            # cascade (the 49¢ HYPE failure). Cancel immediately.
                            avg_c = _live_fill_price_cents(info)
                            if avg_c is not None and avg_c < MAKER_BAILOUT_CENTS:
                                emoji = ASSET_EMOJI.get(asset, "")
                                tg(f"🛑 <b>BAIL-OUT · {emoji} {asset} {info['tf']}m {dirn}</b>\n"
                                   f"Filling at {avg_c:.1f}¢ (< {MAKER_BAILOUT_CENTS:.0f}¢ floor) — "
                                   f"outcome collapsing, cancelling remainder.")
                                threading.Thread(target=_finalize_maker,
                                                 args=(info, f"bail-out: filling at {avg_c:.1f}¢"),
                                                 daemon=True).start()
                                continue
                            if not info["notified_fill"]:
                                info["notified_fill"] = True
                                emoji = ASSET_EMOJI.get(asset, "")
                                tg(f"🪧 <b>Bid filling · {emoji} {asset} {info['tf']}m {dirn}</b>\n"
                                   f"{matched:g}/{info['shares']:g} sh @ {info['price']*100:.0f}¢")
                        if matched >= info["shares"] - 0.01:
                            threading.Thread(target=_finalize_maker, args=(info, "fully filled"), daemon=True).start()
        except Exception as e:
            log.error(f"[MAKER] monitor error: {e}")


def start_maker_monitor():
    if not MAKER_MODE:
        log.info("[MAKER] disabled (MAKER_MODE=false) - taker FAK orders in use")
        return False
    threading.Thread(target=maker_monitor, daemon=True).start()
    log.info(f"[MAKER] monitor started (bid {MAKER_BID_CENTS:.0f}¢ · cancel on {MAKER_CANCEL_REV_PCT}% Binance reversal)")
    return True


# ─── WINDOW LOGIC ────────────────────────────────────────────────────────────
def get_window_times(tf):
    now = datetime.now(timezone.utc)
    slot = (now.minute // tf) * tf
    open_ = now.replace(minute=slot, second=0, microsecond=0)
    cm = slot + tf
    if cm >= 60:
        close_ = open_.replace(hour=(open_.hour + 1) % 24, minute=0)
    else:
        close_ = open_.replace(minute=cm)
    return open_, close_, max(0, int((close_ - now).total_seconds()))


def wkey(asset, tf, open_time):
    return f"{asset}_{tf}_{open_time.strftime('%Y%m%d%H%M')}"


def count_reversals(hist):
    if len(hist) < 3:
        return 0
    revs, prev = 0, None
    pl = list(hist)
    for i in range(1, len(pl)):
        d = pl[i] - pl[i-1]
        if d == 0: continue
        cur = "up" if d > 0 else "dn"
        if prev and cur != prev: revs += 1
        prev = cur
    return revs


def count_reversals_recent(hist, seconds=30):
    """Count reversals in the last N seconds (price history is appended ~1/sec)."""
    if len(hist) < 3:
        return 0
    recent = list(hist)[-seconds:]
    if len(recent) < 3:
        return 0
    revs, prev = 0, None
    for i in range(1, len(recent)):
        d = recent[i] - recent[i-1]
        if d == 0: continue
        cur = "up" if d > 0 else "dn"
        if prev and cur != prev: revs += 1
        prev = cur
    return revs


def measure_volatility_pct(hist, seconds=30):
    """
    Measure recent 'jumpiness': the average absolute per-second price move over the
    last N seconds, expressed as a % of price. Direction-agnostic — just how much
    the price is moving each second. High value = jumpy/unstable market.
    Returns 0.0 if not enough history.
    """
    if len(hist) < 3:
        return 0.0
    recent = list(hist)[-seconds:]
    if len(recent) < 3:
        return 0.0
    total_pct = 0.0
    count = 0
    for i in range(1, len(recent)):
        if recent[i-1] > 0:
            total_pct += abs(recent[i] - recent[i-1]) / recent[i-1] * 100
            count += 1
    return (total_pct / count) if count > 0 else 0.0


def measure_vol_stats(hist, seconds=30, big_jump_pct=0.02):
    """
    Return a dict of volatility statistics over the last N seconds:
      - net:      net % move start→end (signed; direction of the window)
      - avg:      average absolute per-second % move
      - rv:       realized volatility = sqrt(sum of squared per-second % moves)
      - max:      largest single per-second % move
      - big:      count of per-second moves bigger than big_jump_pct
    Used for data collection / analysis (shown on settled trades).
    """
    stats = {"net": 0.0, "avg": 0.0, "rv": 0.0, "max": 0.0, "big": 0}
    if len(hist) < 3:
        return stats
    recent = list(hist)[-seconds:]
    if len(recent) < 3:
        return stats
    pct_moves = []
    for i in range(1, len(recent)):
        if recent[i-1] > 0:
            pct_moves.append(abs(recent[i] - recent[i-1]) / recent[i-1] * 100)
    if not pct_moves:
        return stats
    if recent[0] > 0:
        stats["net"] = (recent[-1] - recent[0]) / recent[0] * 100
    stats["avg"] = sum(pct_moves) / len(pct_moves)
    stats["rv"] = (sum(p * p for p in pct_moves)) ** 0.5
    stats["max"] = max(pct_moves)
    stats["big"] = sum(1 for p in pct_moves if p > big_jump_pct)
    return stats


def measure_move_hold(hist, open_price, seconds=60):
    """MOVE-HOLD instability over the last N seconds (the metric we measure in the
    probe). Looks at the move-from-open — (price - open) — each second and measures
    how much that sequence DECAYS (slides toward zero) or SWINGS (bounces). Low =
    the move held steady (good); high = decaying/swinging (bad). In % of price.
    Returns None if not enough history or no open price."""
    if not open_price or open_price <= 0:
        return None
    if len(hist) < 3:
        return None
    recent = list(hist)[-seconds:]
    if len(recent) < 3:
        return None
    diffs = [(p - open_price) / open_price * 100.0 for p in recent if p > 0]
    if len(diffs) < 3:
        return None
    level = diffs[-1]  # where the move currently sits
    mad = sum(abs(d - level) for d in diffs) / len(diffs)
    return round(mad, 4)


def check_momentum(hist, direction, n):
    if len(hist) < n + 1:
        return True
    r = list(hist)[-(n+1):]
    if direction == "UP":
        return all(r[i] <= r[i+1] for i in range(len(r)-1))
    return all(r[i] >= r[i+1] for i in range(len(r)-1))


def check_correlation(asset, direction):
    pairs = [("BTC", "ETH"), ("ETH", "BTC")]
    for a1, a2 in pairs:
        if asset == a1:
            for tf in TIMEFRAMES:
                if active_directions.get((a2, tf)) == direction:
                    return False, f"{a2} active {direction}"
    return True, None


# ─── POLYMARKET MARKET LOOKUP ────────────────────────────────────────────────
def find_market_for_window(asset, tf, open_time):
    """
    Find the Polymarket condition_id + token_ids for a given crypto Up/Down market.
    Polymarket uses Unix timestamp-based event slugs:
      btc-updown-5m-{timestamp}      (5-min event, timestamp = window start Unix epoch)
      eth-updown-5m-{timestamp}
      btc-updown-15m-{timestamp}     (15-min event)
      eth-updown-15m-{timestamp}
    Note: the slug is an EVENT slug. We query /events?slug=... and pull markets from inside.
    """
    asset_short = ASSET_SLUG.get(asset)
    if not asset_short:
        return None, None, None

    # Convert window open_time to Unix timestamp (seconds since epoch)
    window_ts = int(open_time.timestamp())
    slug = f"{asset_short}-updown-{tf}m-{window_ts}"

    if slug in market_cache:
        return market_cache[slug]

    try:
        # Query the EVENTS endpoint (not markets) since the slug is an event slug
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        res = requests.get(url, timeout=10)
        data = res.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            log.warning(f"Event not found for slug: {slug}")
            return None, None, None
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            log.warning(f"No markets in event: {slug}")
            return None, None, None
        # Up/Down events typically have ONE market with two outcomes (YES=Up, NO=Down)
        market = markets[0]
        condition_id = market.get("conditionId")
        token_ids = market.get("clobTokenIds")
        if isinstance(token_ids, str):
            import json
            token_ids = json.loads(token_ids)
        # token_ids[0] = YES (UP), token_ids[1] = NO (DOWN)
        up_token = token_ids[0]
        down_token = token_ids[1]
        market_cache[slug] = (condition_id, up_token, down_token)
        log.info(f"Found market via event {slug}: condId={condition_id[:10]}...")
        return condition_id, up_token, down_token
    except Exception as e:
        log.warning(f"Market lookup error for {slug}: {e}")
        return None, None, None


# ─── ORDER PLACEMENT (SIGNALS-ONLY MODE) ─────────────────────────────────────
# Polymarket API has a known bug preventing order placement for new accounts
# (signer/API key mismatch). While Polymarket fixes this (ETA Tuesday May 19),
# bot runs in signals-only mode: detects signals, alerts via Telegram with
# market link, you manually trade via Polymarket app.
SIGNALS_ONLY_MODE = os.environ.get("SIGNALS_ONLY_MODE", "false").lower() == "true"

def place_order(asset, tf, direction, bet_size, open_time):
    """In signals-only mode, returns the order book info without placing an order."""
    condition_id, up_token, down_token = find_market_for_window(asset, tf, open_time)
    if not up_token:
        return {"status": "failed", "error": "Market not found"}

    token_id = up_token if direction == "UP" else down_token

    try:
        # Use get_price() not get_order_book() - the book endpoint returns
        # stale "ghost" data (always shows 99¢/1¢). get_price returns the
        # actual current ask price. (Known SDK bug in py-clob-client)
        # side="SELL" gives the ask (what we'd PAY to buy)
        price_resp = clob_client.get_price(token_id, side="SELL")
        if isinstance(price_resp, dict):
            best_ask = float(price_resp.get("price", 0))
        else:
            best_ask = float(price_resp)

        if best_ask <= 0:
            return {"status": "failed", "error": "Invalid ask price"}

        entry_cents = best_ask * 100

        min_cents, max_cents = entry_range_for(asset)

        if entry_cents < min_cents:
            return {
                "status": "skipped",
                "error": f"Ask {entry_cents:.1f}¢ < min {min_cents:.0f}¢ (market disagrees)",
                "entry_cents": entry_cents,
            }

        if entry_cents > max_cents:
            return {
                "status": "skipped",
                "error": f"Ask {entry_cents:.1f}¢ > max {max_cents:.0f}¢",
                "entry_cents": entry_cents,
            }

        # Polymarket API decimal precision rules:
        # - Maker amount (cost in USDC): max 2 decimal places
        # - Taker amount (shares): max 4 decimal places
        # We round shares to match these constraints.
        # Round shares DOWN to avoid spending more than bet_size
        import math
        # Round price to 2 decimals (cents)
        best_ask = round(best_ask, 2)
        # Calculate shares such that bet_size = shares * best_ask is clean
        # Trying to keep cost at exactly bet_size (rounded to 2 decimals)
        shares = math.floor((bet_size / best_ask) * 100) / 100  # 2 decimal places
        # Make sure we don't exceed bet_size
        if shares * best_ask > bet_size:
            shares -= 0.01
        shares = round(shares, 2)
        if shares < 0.01:
            return {"status": "failed", "error": f"Calculated shares too small: {shares}"}

        if SIGNALS_ONLY_MODE:
            # Build market URL for manual trading
            asset_short = ASSET_SLUG.get(asset, asset.lower())
            window_ts = int(open_time.timestamp())
            market_url = f"https://polymarket.com/event/{asset_short}-updown-{tf}m-{window_ts}"
            return {
                "status": "signal",
                "shares": shares,
                "entry_cents": entry_cents,
                "market_url": market_url,
                "order_id": "SIGNAL_ONLY",
            }

        # === REAL ORDER PLACEMENT ===
        # Use MarketOrderArgs with amount (USDC) instead of size (shares).
        # This lets the SDK handle all decimal precision internally.
        # FAK (Fill-And-Kill) allows partial fills, more forgiving than FOK.
        from py_clob_client_v2 import MarketOrderArgs
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=bet_size,  # USDC amount, not shares
            side=BUY,
            order_type=OrderType.FAK,
        )
        resp = clob_client.create_and_post_market_order(
            order_args=order_args,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FAK,
        )
        log.info(f"Order response: {resp}")

        success = False
        actual_shares = shares  # default to estimated
        actual_cost = bet_size  # default to estimated
        if isinstance(resp, dict):
            success = resp.get("success", False) or resp.get("status") == "matched"
            order_id = resp.get("orderID") or resp.get("orderId", "unknown")
            # Get ACTUAL fill amounts from Polymarket response
            if "takingAmount" in resp:
                try:
                    actual_shares = float(resp["takingAmount"])
                except:
                    pass
            if "makingAmount" in resp:
                try:
                    actual_cost = float(resp["makingAmount"])
                except:
                    pass
        else:
            success = getattr(resp, "success", False)
            order_id = getattr(resp, "orderID", "unknown")

        # Calculate REAL entry price from actual fill (not pre-trade ask)
        if actual_shares > 0 and actual_cost > 0:
            real_entry_cents = (actual_cost / actual_shares) * 100
        else:
            real_entry_cents = best_ask * 100

        return {
            "status": "filled" if success else "failed",
            "shares": actual_shares,
            "entry_cents": real_entry_cents,
            "actual_cost": actual_cost,
            "order_id": order_id,
            "raw": str(resp)[:200],
        }
    except Exception as e:
        log.error(f"Order placement error: {e}")
        return {"status": "failed", "error": str(e)}


# ─── TRADING LOOP ────────────────────────────────────────────────────────────
def process_tick():
    for tf in TIMEFRAMES:
        open_time, close_time, secs_left = get_window_times(tf)
        thresh = ASSET_THRESHOLDS_5 if tf == 5 else ASSET_THRESHOLDS_15
        max_revs = MAX_REVERSALS_5 if tf == 5 else MAX_REVERSALS_15

        for asset in ASSET_LIST:
            price = prices.get(asset)
            if not price:
                continue

            threshold = thresh[asset]
            wk_key = wkey(asset, tf, open_time)
            ws_key = (asset, tf)

            if ws_key not in windows or windows[ws_key]["key"] != wk_key:
                prev = windows.get(ws_key)
                if prev and prev["traded"] and prev["trade_db_id"]:
                    settle(prev, asset, tf, price)
                if ws_key in active_directions:
                    del active_directions[ws_key]
                windows[ws_key] = {
                    "key": wk_key, "open_time": open_time, "close_time": close_time,
                    "open_price": price, "traded": False, "skipped": False,
                    "trade_db_id": None, "direction": None,
                    "last_retry_at": 0,
                }
                price_histories[ws_key].clear()

            price_histories[ws_key].append(price)
            w = windows[ws_key]
            pct = (price - w["open_price"]) / w["open_price"] * 100
            absp = abs(pct)
            dirn = "UP" if pct >= 0 else "DOWN"

            if w["skipped"]:
                continue
            # ── STACKING ──
            # Once the window has an entry, allow MORE clips into it while the
            # move still clears the gate, spaced by STACK_GAP_SECS, up to
            # STACK_MAX. This re-enters as certainty holds (the wallet's stacking).
            if w["traded"]:
                if w.get("stacks", 1) >= STACK_MAX:
                    continue
                if time.time() - w.get("last_stack_at", 0) < STACK_GAP_SECS:
                    continue
                # else fall through and re-check the gate for another clip

            # Per-asset-per-tf entry window (ETH/BNB/SOL/HYPE 5m can be tuned)
            entry_window = CONFIG["entry_window_seconds"]
            if tf == 5:
                if asset == "ETH":
                    entry_window = int(os.environ.get("ENTRY_SECS_ETH_5", "40"))
                elif asset == "BNB":
                    entry_window = int(os.environ.get("ENTRY_SECS_BNB_5", "40"))
                elif asset == "SOL":
                    entry_window = int(os.environ.get("ENTRY_SECS_SOL_5", "40"))
                elif asset == "HYPE":
                    # Defaults to the global ENTRY_SECS if ENTRY_SECS_HYPE_5 unset.
                    entry_window = int(os.environ.get("ENTRY_SECS_HYPE_5", str(CONFIG["entry_window_seconds"])))

            if secs_left <= 0 or secs_left > entry_window:
                continue

            # ── FRONTIER GATE (the variant's core difference from the control) ──
            # Enter only if the move clears the lock threshold for the time left,
            # from 323k settled samples. HYPE is held to a stricter floor because
            # it settles on Chainlink spot while we signal on Binance futures, so
            # only large moves overwhelm the basis noise (matches how the
            # profitable wallet trades HYPE: his entries are all 0.20%+).
            if not frontier_locked(asset, absp, secs_left, tf):
                continue
            if state["paused"]:
                continue
            if state["balance_usd"] < CONFIG["min_balance"]:
                state["paused"] = True
                state["pause_reason"] = "Balance too low"
                tg(f"🛑 <b>Balance ${state['balance_usd']:.2f} < ${CONFIG['min_balance']}</b>\nPaused.")
                continue

            # Rate-limit retry attempts - only try every N seconds
            now_ts = time.time()
            if now_ts - w["last_retry_at"] < CONFIG["retry_interval_secs"]:
                continue
            w["last_retry_at"] = now_ts

            corr_ok, corr_reason = check_correlation(asset, dirn)
            if not corr_ok:
                w["skipped"] = True
                continue

            # One-asset-at-a-time limit: block if a DIFFERENT asset has an open
            # position. Same asset on the other timeframe (5m/15m) is allowed.
            # Caps simultaneous exposure during correlated/volatile markets.
            if CONFIG["one_asset_at_a_time"]:
                open_assets = {a for (a, _tf) in active_directions.keys()}
                if open_assets and asset not in open_assets:
                    w["skipped"] = True
                    log.info(f"Skipped {asset} {tf}m - another asset already open: {open_assets}")
                    continue

            hist = price_histories[ws_key]
            revs = count_reversals(hist)
            recent_revs = count_reversals_recent(hist, seconds=30)
            volatility = measure_volatility_pct(hist, seconds=30)
            emoji = ASSET_EMOJI.get(asset, "")
            arrow = "🔺" if dirn == "UP" else "🔻"

            # Volatility filter. Always LOG the measured value (data collection).
            # Only SKIP if MAX_VOLATILITY_PCT > 0 (i.e. you've turned it on).
            log.info(f"[VOLATILITY] {asset} {tf}m vol={volatility:.4f}%/s (30s) move={pct:+.3f}%")
            if MAX_VOLATILITY_PCT > 0 and volatility > MAX_VOLATILITY_PCT:
                w["skipped"] = True
                tg(
                    f"⏭ <b>SKIPPED · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
                    f"📈 Move: {pct:+.3f}%\n"
                    f"🌊 Reason: Too volatile ({volatility:.4f}%/s > {MAX_VOLATILITY_PCT:.4f}%/s)\n"
                    f"⏱ {secs_left}s left"
                )
                log.info(f"Skipped {asset} {tf}m {dirn} - too volatile ({volatility:.4f}%/s)")
                continue

            if (not MAKER_MODE) and revs > max_revs:
                w["skipped"] = True
                tg(
                    f"⏭ <b>SKIPPED · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
                    f"📈 Move: {pct:+.3f}%\n"
                    f"🔄 Reason: Too many reversals in window ({revs}/{max_revs})\n"
                    f"📊 30s reversals: {recent_revs}\n"
                    f"⏱ {secs_left}s left"
                )
                continue

            # Momentum check removed in v3.5.5 - too aggressive, blocked strong signals
            # over single-tick bounces. Reversal filters above + choppy filter below
            # already protect against noise.

            # Conviction check (5min only): skip if move is weakening significantly (reversal in progress)
            # Compares move % now vs move % ~30 seconds ago
            # Tolerance is per-asset (ETH is tighter due to more losses)
            # MAKER MODE: skipped - the live Binance cancel trigger does this job better.
            if tf == 5 and not MAKER_MODE:
                # Per-asset weakening tolerance (in % points)
                weakening_tolerance_map = {
                    "BTC":  float(os.environ.get("WEAKENING_TOLERANCE_BTC",  "0.05")),
                    "ETH":  float(os.environ.get("WEAKENING_TOLERANCE_ETH",  "0.025")),
                    "SOL":  float(os.environ.get("WEAKENING_TOLERANCE_SOL",  "0.025")),
                    "DOGE": float(os.environ.get("WEAKENING_TOLERANCE_DOGE", "0.05")),
                    "BNB":  float(os.environ.get("WEAKENING_TOLERANCE_BNB",  "0.025")),
                }
                weakening_tolerance = weakening_tolerance_map.get(asset, 0.05)
                if len(hist) >= 30:
                    price_30s_ago = list(hist)[-30]
                    move_30s_ago = (price_30s_ago - w["open_price"]) / w["open_price"] * 100
                    weakening = abs(move_30s_ago) - abs(pct)
                    # Same direction check (both negative for DOWN, both positive for UP)
                    same_direction = (move_30s_ago < 0 and pct < 0) or (move_30s_ago > 0 and pct > 0)
                    if same_direction and weakening > weakening_tolerance:
                        w["skipped"] = True
                        tg(
                            f"⏭ <b>SKIPPED · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
                            f"📈 Move now: {pct:+.3f}%\n"
                            f"📉 Move 30s ago: {move_30s_ago:+.3f}%\n"
                            f"⚠️ Reason: Move weakening by {weakening:.3f}% (>{weakening_tolerance:.3f}% tolerance)\n"
                            f"📊 30s reversals: {recent_revs}\n"
                            f"⏱ {secs_left}s left"
                        )
                        continue

            # Choppy filter: skip if too many reversals in last 30 seconds
            # ETH gets a tighter threshold (15) than others (20 default)
            choppy_limit = CONFIG["choppy_threshold"]
            if asset == "ETH":
                choppy_limit = int(os.environ.get("CHOPPY_THRESHOLD_ETH", "15"))
            if (not MAKER_MODE) and recent_revs >= choppy_limit:
                w["skipped"] = True
                tg(
                    f"⏭ <b>SKIPPED · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
                    f"📈 Move: {pct:+.3f}%\n"
                    f"🌪 Reason: Too choppy ({recent_revs}/{choppy_limit} reversals in 30s)\n"
                    f"⏱ {secs_left}s left"
                )
                log.info(f"Skipped {asset} {tf}m {dirn} - too choppy ({recent_revs} reversals in 30s)")
                continue

            # Log the reversal counts for entered trades (for tuning the choppy threshold)
            vol_stats = measure_vol_stats(hist, seconds=30)
            move_hold = measure_move_hold(hist, w["open_price"], seconds=60)
            log.info(f"ENTER {asset} {tf}m {dirn} - window_revs={revs}/{max_revs}, 30s_revs={recent_revs}/{CONFIG['choppy_threshold']}, move={pct:+.3f}%, hold={move_hold}, net={vol_stats['net']:+.4f}% avg={vol_stats['avg']:.4f} rv={vol_stats['rv']:.4f} big={vol_stats['big']}")
            w["entry_volatility"] = volatility
            w["entry_vol_stats"] = vol_stats
            w["entry_hold"] = move_hold
            enter_trade(w, asset, tf, price, pct, dirn, secs_left, open_time, close_time, revs, recent_revs)


def enter_trade(w, asset, tf, price, pct, dirn, secs_left, open_time, close_time, revs=0, recent_revs=0):
    bet = min(current_bet_size(), state["balance_usd"])
    if bet < CONFIG["min_balance"]:
        return

    # ── MAKER PATH: rest a limit bid at 99¢ and let sellers come to us ──
    if MAKER_MODE and not SIGNALS_ONLY_MODE:
        emoji = ASSET_EMOJI.get(asset, "")
        arrow = "🔺" if dirn == "UP" else "🔻"
        result = place_maker_order(asset, tf, dirn, bet, open_time, close_time, w)
        if result["status"] == "skipped":
            # Best ask below the floor - market disagrees with the signal. Log and
            # skip the window; no Telegram spam (same as taker-mode skips).
            w["skipped"] = True
            log.info(f"[MAKER] skipped {asset} {tf}m {dirn}: {result['error']}")
            db_log_skip(asset, tf, dirn, pct, w["open_price"], 0,
                        result["error"], open_time, close_time, secs_left)
            return
        if result["status"] == "min_size":
            # Bet too small for Polymarket's 5-share limit-order minimum. Skip this
            # window (retrying won't change the bet) and warn once per boot.
            w["skipped"] = True
            global _min_size_warned
            if not _min_size_warned:
                _min_size_warned = True
                tg(
                    f"⚠️ <b>Bet too small for maker orders</b>\n\n"
                    f"Polymarket limit orders need ≥{MAKER_MIN_SHARES:g} shares "
                    f"(${MAKER_MIN_SHARES*MAKER_BID_CENTS/100:.2f} at {MAKER_BID_CENTS:.0f}¢).\n"
                    f"Current bet: ${bet:.2f}\n"
                    f"Set BET_SIZE_DAY / BET_SIZE_NIGHT to {MAKER_MIN_SHARES*MAKER_BID_CENTS/100:.2f} or more."
                )
            log.warning(f"[MAKER] {result['error']}")
            return
        if result["status"] == "resting":
            w["traded"] = True
            w["direction"] = dirn
            w["entry_move"] = pct
            w["entry_revs"] = revs
            w["entry_recent_revs"] = recent_revs
            w["stacks"] = w.get("stacks", 0) + 1
            w["last_stack_at"] = time.time()
            active_directions[(asset, tf)] = dirn
            sx = f" ×{w['stacks']}" if w["stacks"] > 1 else ""
            tg(f"🪧 {emoji}{asset} {tf}m {dirn}{arrow}{sx} · {pct:+.3f}% · "
               f"{result['shares']:g}sh@99¢ · {secs_left}s")
        else:
            # Failed (market not found, stale feed, rejected) - allow retry next cycle
            log.warning(f"[MAKER] place failed (will retry): {result.get('error')}")
        return

    result = place_order(asset, tf, dirn, bet, open_time)

    emoji = ASSET_EMOJI.get(asset, "")
    arrow = "🔺" if dirn == "UP" else "🔻"

    if result["status"] == "signal":
        # Signals-only mode - alert user with market info for manual trading
        w["traded"] = True  # Mark as traded so we don't fire again for this window
        now = datetime.now(timezone.utc)
        # Save to DB as a "signal" record
        # In signals-only mode, "cost" is the hypothetical cost (what we WOULD have spent)
        # so PnL math correctly reflects what a real trade would have earned/lost.
        trade = {
            "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
            "direction": dirn, "pct_move": round(pct, 4),
            "open_price": w["open_price"], "entry_price": price,
            "real_entry_cents": round(result["entry_cents"], 1),
            "shares": result["shares"], "cost": bet,  # hypothetical cost for PnL accuracy
            "order_id": "SIGNAL_ONLY",
            "window_open": open_time.isoformat(),
            "window_close": close_time.isoformat(),
        }
        db_id = db_insert_trade(trade)
        w["trade_db_id"] = db_id
        w["direction"] = dirn
        active_directions[(asset, tf)] = dirn

        tg(
            f"🚨 <b>SIGNAL · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
            f"📈 Move: {pct:+.3f}%\n"
            f"⏱ {secs_left}s left\n"
            f"📊 30s reversals: {recent_revs}/{CONFIG['choppy_threshold']}\n"
            f"💰 Polymarket ask: <b>{result['entry_cents']:.1f}¢</b>\n"
            f"📦 Would buy: {result['shares']} shares\n"
            f"💵 Cost: ${bet:.2f}\n\n"
            f"<a href=\"{result['market_url']}\">🔗 Trade on Polymarket</a>\n"
            f"🕐 {est_str()}"
        )
        return

    if result["status"] == "skipped":
        # Entry too expensive or too cheap - log to DB silently, no Telegram spam
        w["skipped"] = True
        log.info(f"Skipped {asset} {tf}m {dirn}: {result['error']}")
        db_log_skip(
            asset, tf, dirn, pct, w["open_price"],
            result.get("entry_cents", 0), result["error"],
            open_time, close_time, secs_left
        )
        return

    if result["status"] == "filled":
        w["traded"] = True
        now = datetime.now(timezone.utc)
        # Use ACTUAL cost from the fill (partial fills cost less than intended bet).
        # This keeps PnL accurate - otherwise a partially-filled win looks like a loss.
        actual_cost = result.get("actual_cost", bet)
        if actual_cost <= 0:
            actual_cost = bet
        trade = {
            "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
            "direction": dirn, "pct_move": round(pct, 4),
            "open_price": w["open_price"], "entry_price": price,
            "real_entry_cents": round(result["entry_cents"], 1),
            "shares": result["shares"], "cost": actual_cost,
            "order_id": result["order_id"],
            "window_open": open_time.isoformat(),
            "window_close": close_time.isoformat(),
        }
        db_id = db_insert_trade(trade)
        w["trade_db_id"] = db_id
        w["direction"] = dirn
        # Stash entry conditions so the settlement message can show them.
        w["entry_move"] = pct
        w["entry_revs"] = revs
        w["entry_recent_revs"] = recent_revs
        state["balance_usd"] -= actual_cost
        state["trades_today"] += 1
        active_directions[(asset, tf)] = dirn

        # Track this position for stop loss monitoring
        condition_id, up_token, down_token = find_market_for_window(asset, tf, open_time)
        token_id = up_token if dirn == "UP" else down_token
        open_positions[db_id] = {
            "token_id": token_id,
            "shares": result["shares"],
            "entry_cents": result["entry_cents"],
            "asset": asset,
            "tf": tf,
            "direction": dirn,
            "close_time": close_time,
            "stopped": False,
        }

        tg(
            f"{arrow} <b>{emoji} {asset} · {tf}m · {dirn}</b>\n\n"
            f"📈 Move: {pct:+.3f}%\n"
            f"⏱ {secs_left}s left · {revs_str(w, asset, tf)} rev\n"
            f"💵 ${bet} @ <b>{result['entry_cents']:.1f}¢</b>\n"
            f"📦 {result['shares']} shares\n"
            f"🛑 Stop loss: {CONFIG['stop_loss_cents']:.0f}¢\n"
            f"🆔 <code>{result['order_id'][:12]}...</code>\n"
            f"🕐 {est_str()}"
        )
    else:
        # Failed (empty book, network error, etc) - DON'T mark traded, allow retries
        err = result.get("error", "unknown")
        log.warning(f"Trade attempt failed (will retry): {err}")
        # Detect geoblock - this is fatal, pause bot
        if "403" in err or "restricted" in err.lower() or "geoblock" in err.lower():
            w["skipped"] = True
            state["paused"] = True
            state["pause_reason"] = "Geoblocked by Polymarket"
            tg(
                f"🚫 <b>GEOBLOCKED</b>\n\n"
                f"Polymarket is blocking the bot's server IP.\n"
                f"Trading paused. Need VPS in allowed region.\n"
                f"<code>{err[:200]}</code>"
            )


def revs_str(w, asset, tf):
    ws_key = (asset, tf)
    return count_reversals(price_histories[ws_key])


def fetch_polymarket_outcome(asset, tf, open_time, retry_count=0):
    """
    Fetch the ACTUAL settled outcome from Polymarket (Chainlink-based) rather than
    calculating it from our price sources.

    Returns:
        "UP" if Polymarket settled the market as UP/YES
        "DOWN" if Polymarket settled as DOWN/NO
        None if not yet settled or error
    """
    asset_short = ASSET_SLUG.get(asset)
    if not asset_short:
        return None

    window_ts = int(open_time.timestamp())
    slug = f"{asset_short}-updown-{tf}m-{window_ts}"

    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        res = requests.get(url, timeout=10)
        data = res.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            log.warning(f"Outcome fetch: event not found for {slug}")
            return None
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            log.warning(f"Outcome fetch: no markets in event {slug}")
            return None
        market = markets[0]

        # Polymarket markets have a "outcomePrices" field that resolves to ["1","0"] or ["0","1"]
        # after settlement. Index 0 = UP (YES), Index 1 = DOWN (NO).
        outcome_prices = market.get("outcomePrices")
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except:
                pass

        if not outcome_prices or len(outcome_prices) < 2:
            log.info(f"Outcome fetch: {slug} no outcomePrices yet")
            return None

        # Check outcome prices directly - they resolve to 1/0 or 0/1 once settled
        # Don't rely on closed/umaResolutionStatus flags since Chainlink markets
        # may not set these the same way as UMA-resolved markets
        try:
            up_price = float(outcome_prices[0])
            down_price = float(outcome_prices[1])
        except (ValueError, TypeError):
            log.warning(f"Outcome fetch: invalid prices for {slug}: {outcome_prices}")
            return None

        # During trading, prices are between 0 and 1 (e.g., 0.65, 0.35).
        # After settlement, the winner is exactly 1.0 and loser is 0.0
        if up_price >= 0.99:
            log.info(f"Outcome fetch: {slug} settled UP (prices: {outcome_prices})")
            return "UP"
        elif down_price >= 0.99:
            log.info(f"Outcome fetch: {slug} settled DOWN (prices: {outcome_prices})")
            return "DOWN"
        else:
            # Not yet settled (prices still showing live market sentiment)
            log.info(f"Outcome fetch: {slug} not yet settled (prices: UP={up_price}, DOWN={down_price})")
            return None
    except Exception as e:
        log.warning(f"Outcome fetch error for {slug}: {e}")
        return None


# ─── STOP LOSS MONITORING ────────────────────────────────────────────────────
def get_position_value_cents(token_id):
    """
    Get the current best bid price (in cents) for our token.
    This is what we could SELL for right now.
    Returns None if no bids available.
    """
    if not clob_client:
        return None
    try:
        order_book = clob_client.get_order_book(token_id)
        if not order_book:
            return None
        if isinstance(order_book, dict):
            bids = order_book.get("bids", [])
        else:
            bids = getattr(order_book, "bids", [])
        if not bids:
            return None
        first_bid = bids[0]
        if isinstance(first_bid, dict):
            best_bid = float(first_bid.get("price", 0))
        else:
            best_bid = float(getattr(first_bid, "price", 0))
        return best_bid * 100
    except Exception as e:
        log.warning(f"Position value check error: {e}")
        return None


def place_sell_order(token_id, shares, price):
    """Place a SELL order at the given price. Returns success bool + order_id."""
    if not clob_client or not V2_AVAILABLE:
        return False, None
    try:
        from py_clob_client_v2.order_builder.constants import SELL
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=shares,
            side=SELL,
        )
        resp = clob_client.create_and_post_order(
            order_args,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.GTC,  # Good-til-cancelled, not FOK
        )
        log.info(f"Sell order response: {resp}")
        if isinstance(resp, dict):
            success = resp.get("success", False) or resp.get("status") == "matched"
            order_id = resp.get("orderID") or resp.get("orderId", "unknown")
        else:
            success = getattr(resp, "success", False)
            order_id = getattr(resp, "orderID", "unknown")
        return success, order_id
    except Exception as e:
        log.error(f"Sell order error: {e}")
        return False, None


def stop_loss_monitor():
    """
    Background thread that checks open positions every N seconds.
    If a position's value drops to or below stop loss threshold, sell it.
    """
    while True:
        try:
            time.sleep(CONFIG["stop_loss_check_secs"])

            if not open_positions:
                continue

            stop_cents = CONFIG["stop_loss_cents"]
            discount = CONFIG["stop_loss_discount"]

            for db_id, pos in list(open_positions.items()):
                if pos.get("stopped"):
                    continue

                # Don't stop loss if window is about to close anyway (last 30s)
                secs_to_close = (pos["close_time"] - datetime.now(timezone.utc)).total_seconds()
                if secs_to_close < 30:
                    continue

                current_value = get_position_value_cents(pos["token_id"])
                if current_value is None:
                    continue

                if current_value <= stop_cents:
                    # Trigger stop loss
                    log.warning(f"STOP LOSS TRIGGERED: {pos['asset']} {pos['tf']}m {pos['direction']} - value {current_value:.1f}¢ <= {stop_cents}¢")

                    # Place sell at a discount to ensure fill
                    sell_price = max(0.01, (current_value / 100) * (1 - discount))
                    success, order_id = place_sell_order(pos["token_id"], pos["shares"], sell_price)

                    pos["stopped"] = True  # Mark stopped regardless to avoid retry spam

                    emoji = ASSET_EMOJI.get(pos["asset"], "")
                    if success:
                        proceeds = pos["shares"] * sell_price
                        loss = (pos["entry_cents"] / 100 * pos["shares"]) - proceeds
                        tg(
                            f"🛑 <b>STOP LOSS · {emoji} {pos['asset']} {pos['tf']}m</b>\n\n"
                            f"Entry: {pos['entry_cents']:.1f}¢ → Sold: {sell_price*100:.1f}¢\n"
                            f"Loss: ~${loss:.3f}\n"
                            f"🆔 <code>{str(order_id)[:12]}</code>\n"
                            f"🕐 {est_str()}"
                        )
                    else:
                        tg(
                            f"⚠️ <b>STOP LOSS FAILED · {emoji} {pos['asset']} {pos['tf']}m</b>\n\n"
                            f"Tried to sell at {sell_price*100:.1f}¢ but order failed.\n"
                            f"Position will ride to window close."
                        )
        except Exception as e:
            log.error(f"Stop loss monitor error: {e}")


def start_stop_loss_thread():
    t = threading.Thread(target=stop_loss_monitor, daemon=True)
    t.start()
    log.info(f"Stop loss monitor started (threshold {CONFIG['stop_loss_cents']}¢)")


def settle(w, asset, tf, close_price):
    """
    Schedules settlement in a background thread so the main loop isn't blocked.
    The settlement retries fetching Polymarket's outcome for up to 60 seconds.
    Guarded so the same window can never be settled twice (rollover + maker finalize).
    """
    if w.get("_settle_started"):
        return
    w["_settle_started"] = True
    # Capture state we need (in case window state changes during settlement)
    settle_data = {
        "trade_db_id": w["trade_db_id"],
        "direction": w["direction"],
        "open_time": w["open_time"],
        "open_price": w["open_price"],
        "close_price": close_price,
        "entry_move": w.get("entry_move"),
        "entry_revs": w.get("entry_revs"),
        "entry_recent_revs": w.get("entry_recent_revs"),
        "entry_volatility": w.get("entry_volatility"),
        "entry_vol_stats": w.get("entry_vol_stats"),
        "entry_hold": w.get("entry_hold"),
        "close_time": w.get("close_time"),
    }
    t = threading.Thread(target=_settle_worker, args=(settle_data, asset, tf), daemon=True)
    t.start()


def _settle_worker(d, asset, tf):
    """Background worker that does the actual settlement with retries.
    Polymarket Gamma API can be slow (5-30 min). Retry that long.
    NEVER fall back to local Chainlink math - timing differences cause wrong outcomes.
    If Gamma never returns, mark UNVERIFIED rather than guess.
    """
    # Retry fetching Polymarket outcome for up to 30 minutes
    poly_outcome = None
    max_retries = 180  # 180 attempts × 10 sec = 30 minutes
    retry_delay = 10

    for attempt in range(max_retries):
        poly_outcome = fetch_polymarket_outcome(asset, tf, d["open_time"])
        if poly_outcome:
            break
        if attempt < max_retries - 1:
            if attempt % 6 == 0:  # Log every minute
                log.info(f"Outcome not ready for {asset} {tf}m, retrying (attempt {attempt+1}/{max_retries})...")
            time.sleep(retry_delay)

    if poly_outcome:
        actual = poly_outcome
        source = "Polymarket"
    else:
        # Polymarket Gamma never returned outcome after 30 min - mark UNVERIFIED.
        # Do NOT fall back to Chainlink math - it gives wrong outcomes.
        log.error(f"Settled {asset} {tf}m UNVERIFIED - Polymarket Gamma never published outcome after 30min")
        conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
        conn.execute("UPDATE trades SET result=?, profit_loss=0, balance_after=? WHERE id=?",
                     ("UNVERIFIED", state["balance_usd"], d["trade_db_id"]))
        conn.commit()
        conn.close()
        # Remove from open positions tracking
        if d["trade_db_id"] in open_positions:
            del open_positions[d["trade_db_id"]]
        emoji = ASSET_EMOJI.get(asset, "")
        tg(
            f"❓ <b>{emoji} {asset} {tf}m · UNVERIFIED</b>\n\n"
            f"Polymarket Gamma never returned outcome.\n"
            f"Check Polymarket portfolio for real result."
        )
        return

    won = actual == d["direction"]

    conn = sqlite3.connect(os.environ.get("DB_PATH", "trades_variant.db"))
    c = conn.cursor()
    c.execute("SELECT real_entry_cents, cost, shares FROM trades WHERE id=?", (d["trade_db_id"],))
    row = c.fetchone()
    conn.close()
    entry_c, cost, shares = (row or (99.0, CONFIG["bet_size"], 0))

    if won:
        # Each share pays out $1.00 (100¢)
        payout = shares * 1.0
        pl = payout - cost
    else:
        payout = 0
        pl = -cost

    # Refresh real balance from Polymarket
    check_balance(force=True)
    state["pnl_today"] += pl
    if won:
        state["wins_today"] += 1
        state["consecutive_losses"] = 0  # Reset on win
    else:
        state["losses_today"] += 1
        state["consecutive_losses"] += 1

    result = "WON" if won else "LOST"
    db_settle(d["trade_db_id"], d["close_price"], result, payout, pl, state["balance_usd"])

    # Remove from open positions (no longer needs stop loss monitoring)
    if d["trade_db_id"] in open_positions:
        del open_positions[d["trade_db_id"]]

    total, wins, total_pnl = db_stats()
    wr = f"{wins/total*100:.0f}%" if total > 0 else "—"
    emoji = ASSET_EMOJI.get(asset, "")
    out_emoji = "✅" if won else "❌"

    # Entry conditions on the result line, so a LOSS carries its own numbers for
    # forensics: what was the move and move-hold when it entered.
    em_pct = d.get("entry_move")
    em_hold = d.get("entry_hold")
    cond_bits = []
    if em_pct is not None:
        cond_bits.append(f"{em_pct:+.3f}%")
    if em_hold is not None:
        cond_bits.append(f"hold {em_hold:.3f}")
    cond_str = (" · entry: " + " · ".join(cond_bits)) if cond_bits else ""

    tg(f"{out_emoji} {emoji}{asset} {tf}m {d['direction']} · ${pl:+.3f} · "
       f"bal ${state['balance_usd']:.2f} · {total}·{wr}{cond_str}")

    # Auto-pause after consecutive losses
    if not won and state["consecutive_losses"] >= CONFIG["consecutive_loss_limit"]:
        state["paused"] = True
        state["pause_reason"] = f"{state['consecutive_losses']} losses in a row"
        tg(
            f"⏸ <b>Auto-paused</b>\n"
            f"{state['consecutive_losses']} consecutive losses.\n"
            f"/resume when ready."
        )


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    global prices, last_prices_time
    init_db()

    # Start Chainlink WebSocket background threads for accurate settlement-matching prices
    chainlink_ok = start_chainlink_threads()

    # Start stop loss monitoring thread
    start_stop_loss_thread()

    # Start Binance lag measurement (measure-only; hourly Telegram reports)
    lag_ok = start_lag_measurement()

    # Maker mode needs the Binance feed for cancel triggers, even if lag reports are off
    if MAKER_MODE:
        start_binance_feed()
    maker_ok = start_maker_monitor()

    mode_str = "🚨 SIGNALS-ONLY" if SIGNALS_ONLY_MODE else (
        f"🪧 MAKER - resting {MAKER_BID_CENTS:.0f}¢ bids" if maker_ok else "🤖 TAKER - FAK orders")

    tg(
        f"🧪 <b>PolySniper VARIANT (per-asset gate)</b>\n"
        f"{mode_str}\n"
        f"🎯 <b>PER-MARKET FRONTIER ENTRY</b>\n\n"
        f"Each asset has its own move×time gate:\n"
        f"  BTC lowest · HYPE/SOL highest\n"
        f"  (calm liquid = low bar, volatile = high bar)\n"
        f"🛟 Bail-out: cancel if filling < {MAKER_BAILOUT_CENTS:.0f}¢\n"
        f"💵 Bet: ${CONFIG['bet_size_day']:g} flat\n"
        f"💲 Ask floor: {MAKER_MIN_ASK_CENTS:.0f}¢\n"
        f"🕐 {est_full()}\n\n"
        f"Connecting to Polymarket..."
    )

    if not init_clob():
        log.error("Failed to init Polymarket")
        return

    tg(
        f"✅ <b>Connected!</b>\n\n"
        f"💰 Balance: <b>${state['balance_usd']:.2f}</b>\n"
        f"🎯 Watching {' + '.join(ASSET_LIST)} markets\n"
        f"{mode_str}\n\n"
        f"/help for commands"
    )

    last_price_fetch = 0
    startup_time = time.time()
    last_prices_time = startup_time  # Initialize to startup time, not 0, so we don't fire false warnings
    grace_period = 60  # Don't fire "feed down" warnings for first 60s after startup

    # Heartbeat: periodically log current prices + how stale each is, so you can
    # SEE the feed working. Interval configurable via HEARTBEAT_SECS (0 = off).
    heartbeat_secs = int(os.environ.get("HEARTBEAT_SECS", "10"))
    last_heartbeat = 0

    while True:
        try:
            now = time.time()
            if now - last_price_fetch >= CONFIG["price_interval_secs"]:
                new_p = fetch_validated_prices()
                if new_p:
                    prices = new_p
                    last_prices_time = now
                    if state["pause_reason"] == "Price feed down":
                        state["paused"] = False
                        state["pause_reason"] = None
                        tg("✅ <b>Price feed restored</b>")
                elif now - last_prices_time > 120 and now - startup_time > grace_period and not state["paused"]:
                    state["paused"] = True
                    state["pause_reason"] = "Price feed down"
                    tg("⚠️ <b>Price feed down 2+ min</b>")
                last_price_fetch = now

            handle_commands()
            if prices and not state["paused"]:
                process_tick()

            # Heartbeat: log current prices + staleness so the feed is visible.
            if heartbeat_secs > 0 and now - last_heartbeat >= heartbeat_secs:
                parts = []
                for a in ASSET_LIST:
                    p = prices.get(a)
                    if p is None:
                        parts.append(f"{a}=--")
                    else:
                        age = now - chainlink_last_update.get(a, now)
                        parts.append(f"{a}={p:g}({age:.0f}s)")
                log.info("[Heartbeat] " + "  ".join(parts))
                last_heartbeat = now

            # Periodic balance refresh
            if now - state["last_balance_check"] > 300:
                check_balance()
        except Exception as e:
            log.error(f"Main loop error: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
