#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# TRADER TRACKER  v2 — FULL CAPTURE  (measure-only — NEVER places an order)
#
# Watches ONE trader and records every variable we can honestly measure about
# each of his fills, so you can out-calibrate him from a single run.
#   Target: 0xf3ce251f9c4ae0f3940a9f32de5dd1a1d05b8bc6
#
# CAPTURE (continuous, ~every POLL_SECS, written to SQLite immediately):
#   Tier 1 (his fill, exact): price(4dp), side, outcome, token id, tx, size, usd
#   Tier 2 (market context):  order book at OBSERVATION time (best ask/bid, spread,
#                             depth available at ≤99¢), time-of-day, day-of-week
#   Tier 3 (underlying edge):  Binance move over 3 lookbacks (5s/30s/full window),
#                             realized vol over 10s/30s/60s, Binance-minus-Chainlink
#                             move gap (the actual mispricing), sign-flip count (30s)
#   + settlement: WON / LOST from Polymarket
#   Sequence features (fills-per-window, stacking gaps, cumulative size) are derived
#   at analysis time from the raw rows, so out-of-order polls can't corrupt them.
#
# REPORTING: a summary every REPORT_SECS (default 30 min), plus on-demand commands
#   /report  – force a summary now
#   /recent  – his last individual fills
#   /status  – capture health
#
# HONEST LIMITS:
#   • Polls (~20s) → his fills are seen a few seconds late; underlying moves are
#     reconstructed from our own tick history near his fill timestamp (good to a
#     few seconds, not the millisecond).
#   • The ORDER BOOK is captured at OBSERVATION time (~20s after he filled), NOT at
#     his exact entry — the book has moved. Columns are suffixed _obs and carry a
#     book_age_secs so you know how stale. Useful for liquidity texture, not his
#     exact entry book.
#   • We see FILLS, never intentions: not his unfilled orders, not windows he
#     skipped. His win rate is real, but it's the rate on trades he CHOSE to make.
#   • Places no orders. There is no order-placement code in this file.
# ─────────────────────────────────────────────────────────────────────────────

import os
import re
import time
import json
import math
import bisect
import sqlite3
import logging
import threading
from datetime import datetime, timezone, timedelta
from collections import deque

import requests

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TRADER_WALLET    = os.environ.get(
    "TRADER_WALLET", "0xf3ce251f9c4ae0f3940a9f32de5dd1a1d05b8bc6").strip().lower()

POLL_SECS        = float(os.environ.get("POLL_SECS", "20"))
FETCH_LIMIT      = int(os.environ.get("FETCH_LIMIT", "500"))
REPORT_SECS      = int(os.environ.get("REPORT_SECS", "1800"))   # 30 min default
DB_PATH          = os.environ.get("TRACKER_DB", "trader_tracker.db")
TICK_RETAIN_SECS = int(os.environ.get("TICK_RETAIN_SECS", "1500"))   # ~25 min
CAPTURE_BOOK     = os.environ.get("CAPTURE_BOOK", "true").lower() == "true"

ASSET_SLUG_REV = {"btc": "BTC", "eth": "ETH", "sol": "SOL", "doge": "DOGE",
                  "bnb": "BNB", "xrp": "XRP", "hype": "HYPE"}
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
ALL_ASSETS = list(ASSET_SLUG_REV.values())

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("trader-tracker")

# ─── FEEDS ───────────────────────────────────────────────────────────────────
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@trade" for s in ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "bnbusdt", "xrpusdt"])
BINANCE_SYMBOL_TO_ASSET = {"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL",
                           "DOGEUSDT": "DOGE", "BNBUSDT": "BNB", "XRPUSDT": "XRP"}
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/public/stream?streams=hypeusdt@trade"
BINANCE_FUTURES_SYMBOL_TO_ASSET = {"HYPEUSDT": "HYPE"}

CHAINLINK_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOLS = {"BTC": "btc/usd", "ETH": "eth/usd", "SOL": "sol/usd",
                     "DOGE": "doge/usd", "BNB": "bnb/usd", "XRP": "xrp/usd",
                     "HYPE": "hype/usd"}

binance_hist   = {a: deque() for a in ALL_ASSETS}
chainlink_hist = {a: deque() for a in ALL_ASSETS}
_hist_lock = threading.Lock()

def _push_hist(store, asset, price):
    now = time.time()
    with _hist_lock:
        dq = store[asset]
        dq.append((now, price))
        cutoff = now - TICK_RETAIN_SECS
        while dq and dq[0][0] < cutoff:
            dq.popleft()

def _snapshot(store, asset):
    with _hist_lock:
        return list(store[asset])

def price_at_ticks(ticks, ts, trust=15):
    if not ticks:
        return None
    times = [t for (t, _) in ticks]
    i = bisect.bisect_left(times, ts)
    cands = []
    if i < len(ticks):
        cands.append(ticks[i])
    if i > 0:
        cands.append(ticks[i - 1])
    if not cands:
        return None
    best = min(cands, key=lambda tp: abs(tp[0] - ts))
    return best[1] if abs(best[0] - ts) <= trust else None

def move_pct(ticks, t0, t1):
    p0 = price_at_ticks(ticks, t0)
    p1 = price_at_ticks(ticks, t1)
    if p0 and p1 and p0 > 0:
        return (p1 - p0) / p0 * 100
    return None

def realized_vol_ticks(ticks, t_end, lookback):
    seg = [(t, p) for (t, p) in ticks if t_end - lookback <= t <= t_end]
    if len(seg) < 3:
        return None
    rets = []
    for i in range(1, len(seg)):
        p0 = seg[i - 1][1]
        if p0 > 0:
            rets.append((seg[i][1] - p0) / p0 * 100)
    if not rets:
        return None
    return math.sqrt(sum(r * r for r in rets))

def flip_count_ticks(ticks, t_end, lookback):
    seg = [(t, p) for (t, p) in ticks if t_end - lookback <= t <= t_end]
    if len(seg) < 3:
        return None
    flips, prev = 0, None
    for i in range(1, len(seg)):
        d = seg[i][1] - seg[i - 1][1]
        if d == 0:
            continue
        cur = 1 if d > 0 else -1
        if prev is not None and cur != prev:
            flips += 1
        prev = cur
    return flips

# ─── PUBLIC ORDER BOOK (read-only) ──────────────────────────────────────────
def fetch_book(token_id):
    """Best ask/bid, spread, and shares available at <=99c, from the public CLOB book."""
    try:
        r = requests.get("https://clob.polymarket.com/book",
                         params={"token_id": token_id}, timeout=8)
        b = r.json()
        asks = b.get("asks", []) or []
        bids = b.get("bids", []) or []
        def _p(x):
            return float(x.get("price", 0))
        def _s(x):
            return float(x.get("size", 0))
        best_ask = min((_p(a) for a in asks if _p(a) > 0), default=None)
        best_bid = max((_p(x) for x in bids if _p(x) > 0), default=None)
        depth99 = sum(_s(a) for a in asks if 0 < _p(a) <= 0.99)
        spread = (best_ask - best_bid) if (best_ask and best_bid) else None
        return best_ask, best_bid, spread, round(depth99, 2)
    except Exception as e:
        log.warning(f"[book] {e}")
        return None, None, None, None

# ─── TELEGRAM ────────────────────────────────────────────────────────────────
_tg_offset = None

def tg(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.info(f"[TG-disabled] {msg[:120]}")
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
                      timeout=8)
    except Exception as e:
        log.error(f"TG error: {e}")

def est_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=4)

def est_str():
    return est_now().strftime("%H:%M EST")

# ─── DB ──────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedup_key TEXT UNIQUE,
            tx TEXT, token_id TEXT,
            asset TEXT, tf INTEGER, side TEXT, outcome TEXT,
            price REAL, size REAL, usd REAL,
            fill_ts INTEGER, window_open INTEGER, window_close INTEGER, secs_left INTEGER,
            hour_est INTEGER, dow INTEGER,
            binance_move_window REAL, binance_move_5s REAL, binance_move_30s REAL,
            chainlink_move_window REAL, bin_minus_cl_move REAL,
            rvol_10s REAL, rvol_30s REAL, rvol_60s REAL,
            flip_count_30s INTEGER,
            book_best_ask_obs REAL, book_best_bid_obs REAL,
            book_spread_obs REAL, book_depth99_obs REAL, book_age_secs INTEGER,
            settled_outcome TEXT, won INTEGER,
            seen_at INTEGER
        )
    """)
    conn.commit()
    conn.close()

def db_pending_settlements():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""SELECT DISTINCT asset, tf, window_open FROM fills
                 WHERE settled_outcome IS NULL AND window_close < ?""",
              (int(time.time()) - 30,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_apply_settlement(asset, tf, window_open, outcome):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""UPDATE fills SET settled_outcome=?,
                    won=CASE WHEN outcome=? THEN 1 ELSE 0 END
                    WHERE asset=? AND tf=? AND window_open=?""",
                 (outcome, outcome, asset, tf, window_open))
    conn.commit()
    conn.close()

# ─── FEED WORKERS ────────────────────────────────────────────────────────────
def _binance_loop(url, symmap, label):
    while True:
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=10); ws.settimeout(30)
            log.info(f"[{label}] connected")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                d = json.loads(msg).get("data", {})
                a = symmap.get(d.get("s", ""))
                if a:
                    p = float(d.get("p", 0))
                    if p > 0:
                        _push_hist(binance_hist, a, p)
        except Exception as e:
            log.warning(f"[{label}] {e}")
        finally:
            try: ws and ws.close()
            except Exception: pass
        time.sleep(3)

def _extract_latest_value(payload):
    if not isinstance(payload, dict):
        return None
    arr = payload.get("data")
    if isinstance(arr, list) and arr and isinstance(arr[-1], dict):
        v = float(arr[-1].get("value", 0)); return v if v > 0 else None
    if "value" in payload:
        v = float(payload.get("value", 0)); return v if v > 0 else None
    return None

def chainlink_worker(asset):
    symbol = CHAINLINK_SYMBOLS.get(asset)
    if not symbol:
        return
    reconnect = float(os.environ.get("CL_RECONNECT_SECS", "1.0"))
    while True:
        ws = None
        try:
            ws = websocket.create_connection(CHAINLINK_WS_URL, timeout=10); ws.settimeout(5)
            ws.send(json.dumps({"action": "subscribe", "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "update",
                 "filters": json.dumps({"symbol": symbol})}]}))
            start = time.time()
            while time.time() - start < 5:
                try:
                    msg = ws.recv()
                    if not msg:
                        continue
                    v = _extract_latest_value(json.loads(msg).get("payload", {}))
                    if v is not None:
                        _push_hist(chainlink_hist, asset, v); break
                except websocket.WebSocketTimeoutException:
                    break
                except Exception:
                    break
        except Exception as e:
            log.warning(f"[Chainlink] {asset} {e}")
        finally:
            try: ws and ws.close()
            except Exception: pass
        time.sleep(reconnect)

# ─── PARSE + ENRICH ──────────────────────────────────────────────────────────
SLUG_RE = re.compile(r"^([a-z]+)-updown-(\d+)m-(\d+)$")

def parse_fill(rec):
    slug = rec.get("eventSlug") or rec.get("slug") or ""
    m = SLUG_RE.match(slug)
    if not m:
        return None
    short, tf_s, open_s = m.group(1), m.group(2), m.group(3)
    asset = ASSET_SLUG_REV.get(short)
    if not asset:
        return None
    tf = int(tf_s); window_open = int(open_s); window_close = window_open + tf * 60
    try:
        price = float(rec.get("price", 0)); size = float(rec.get("size", 0))
        fill_ts = int(rec.get("timestamp", 0))
    except (TypeError, ValueError):
        return None
    if price <= 0 or size <= 0 or fill_ts <= 0:
        return None
    outcome = (rec.get("outcome") or "").strip().upper()
    if outcome not in ("UP", "DOWN"):
        idx = rec.get("outcomeIndex")
        outcome = "UP" if idx == 0 else "DOWN" if idx == 1 else ""
    return {
        "dedup_key": f"{rec.get('transactionHash','')}:{asset}:{tf}:{price}:{size}:{fill_ts}",
        "tx": rec.get("transactionHash", ""), "token_id": str(rec.get("asset", "")),
        "asset": asset, "tf": tf, "side": (rec.get("side") or "").strip().upper(),
        "outcome": outcome, "price": price, "size": size, "usd": round(price * size, 2),
        "fill_ts": fill_ts, "window_open": window_open, "window_close": window_close,
        "secs_left": window_close - fill_ts,
    }

def enrich_and_store(f):
    bt = _snapshot(binance_hist, f["asset"])
    ct = _snapshot(chainlink_hist, f["asset"])
    fts, wo = f["fill_ts"], f["window_open"]

    b_win = move_pct(bt, wo, fts)
    b_5s  = move_pct(bt, fts - 5, fts)
    b_30s = move_pct(bt, fts - 30, fts)
    c_win = move_pct(ct, wo, fts)
    bmc   = (b_win - c_win) if (b_win is not None and c_win is not None) else None
    rv10  = realized_vol_ticks(bt, fts, 10)
    rv30  = realized_vol_ticks(bt, fts, 30)
    rv60  = realized_vol_ticks(bt, fts, 60)
    flips = flip_count_ticks(bt, fts, 30)

    ba = bb = bsp = bd = None
    book_age = None
    if CAPTURE_BOOK and f["token_id"]:
        ba, bb, bsp, bd = fetch_book(f["token_id"])
        book_age = int(time.time()) - fts

    dt = datetime.fromtimestamp(fts, timezone.utc) - timedelta(hours=4)  # ET-ish
    hour_est, dow = dt.hour, dt.weekday()

    def r(x, n=4):
        return round(x, n) if x is not None else None

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""INSERT OR IGNORE INTO fills
            (dedup_key, tx, token_id, asset, tf, side, outcome, price, size, usd,
             fill_ts, window_open, window_close, secs_left, hour_est, dow,
             binance_move_window, binance_move_5s, binance_move_30s,
             chainlink_move_window, bin_minus_cl_move,
             rvol_10s, rvol_30s, rvol_60s, flip_count_30s,
             book_best_ask_obs, book_best_bid_obs, book_spread_obs, book_depth99_obs, book_age_secs,
             settled_outcome, won, seen_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,?)""",
            (f["dedup_key"], f["tx"], f["token_id"], f["asset"], f["tf"], f["side"],
             f["outcome"], f["price"], f["size"], f["usd"], fts, wo, f["window_close"],
             f["secs_left"], hour_est, dow,
             r(b_win), r(b_5s), r(b_30s), r(c_win), r(bmc),
             r(rv10), r(rv30), r(rv60), flips,
             r(ba, 4), r(bb, 4), r(bsp, 4), r(bd, 2), book_age,
             int(time.time())))
        new = conn.total_changes > 0
        conn.commit()
    finally:
        conn.close()
    return new

# ─── POLLER ──────────────────────────────────────────────────────────────────
def poll_worker():
    """Polls his wallet every POLL_SECS. If Polymarket rate-limits (HTTP 429) or
    has a server error, backs off exponentially instead of hammering — so you can
    safely set POLL_SECS=1 without risking an IP block that would halt capture.
    Backoff resets to POLL_SECS on the next successful poll."""
    url = "https://data-api.polymarket.com/trades"
    backoff = POLL_SECS
    BACKOFF_CAP = float(os.environ.get("POLL_BACKOFF_CAP", "120"))
    while True:
        try:
            r = requests.get(url, params={"user": TRADER_WALLET, "limit": FETCH_LIMIT,
                                          "takerOnly": "false"}, timeout=15)
            if r.status_code == 429 or r.status_code >= 500:
                backoff = min(max(backoff * 2, 2.0), BACKOFF_CAP)
                log.warning(f"[poll] HTTP {r.status_code} (rate-limited?) — backing off to {backoff:.0f}s")
                tg(f"⚠️ Tracker rate-limited (HTTP {r.status_code}). "
                   f"Auto-slowing to {backoff:.0f}s to avoid a block. "
                   f"Consider raising POLL_SECS.")
                time.sleep(backoff)
                continue
            data = r.json()
            backoff = POLL_SECS  # success — reset to the configured cadence
            if isinstance(data, list):
                his = [rec for rec in data
                       if str(rec.get("proxyWallet", "")).lower() == TRADER_WALLET]
                if not his and data:
                    log.warning("[poll] batch had no records for his wallet — skipping")
                new = 0
                for rec in reversed(his):
                    f = parse_fill(rec)
                    if f and enrich_and_store(f):
                        new += 1
                if new:
                    log.info(f"[poll] stored {new} new fills")
        except Exception as e:
            log.warning(f"[poll] {e}")
        time.sleep(POLL_SECS)

# ─── SETTLEMENT ──────────────────────────────────────────────────────────────
def fetch_outcome(asset, tf, window_open):
    short = next((k for k, v in ASSET_SLUG_REV.items() if v == asset), None)
    if not short:
        return None
    slug = f"{short}-updown-{tf}m-{window_open}"
    try:
        res = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=10)
        data = res.json()
        if not data or not isinstance(data, list):
            return None
        markets = data[0].get("markets", [])
        if not markets:
            return None
        prices = markets[0].get("outcomePrices")
        if isinstance(prices, str):
            prices = json.loads(prices)
        if not prices or len(prices) < 2:
            return None
        up, down = float(prices[0]), float(prices[1])
        if up >= 0.99:
            return "UP"
        if down >= 0.99:
            return "DOWN"
        return None
    except Exception as e:
        log.warning(f"outcome {slug}: {e}")
        return None

def settle_worker():
    while True:
        time.sleep(30)
        try:
            for asset, tf, window_open in db_pending_settlements():
                o = fetch_outcome(asset, tf, window_open)
                if o:
                    db_apply_settlement(asset, tf, window_open, o)
        except Exception as e:
            log.warning(f"[settle] {e}")

# ─── REPORT ──────────────────────────────────────────────────────────────────
SECS_BANDS = [(-5, 3), (3, 6), (6, 10), (10, 20), (20, 40), (40, 70), (70, 99999)]
MOVE_BANDS = [(0.0, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.40), (0.40, 999)]

def _bandkey(bands, v):
    return next(((lo, hi) for (lo, hi) in bands if lo <= v < hi), None)

def build_report():
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT COUNT(*), COALESCE(SUM(usd),0) FROM fills")
    n_all, vol_all = c.fetchone()
    c.execute("SELECT COUNT(*), SUM(won), COALESCE(SUM(usd),0) FROM fills WHERE settled_outcome IS NOT NULL")
    n_set, n_won, vol_set = c.fetchone()
    c.execute("SELECT secs_left, won FROM fills WHERE settled_outcome IS NOT NULL")
    settled = c.fetchall()
    c.execute("""SELECT ABS(binance_move_window), won FROM fills
                 WHERE settled_outcome IS NOT NULL AND binance_move_window IS NOT NULL""")
    with_move = c.fetchall()
    c.execute("""SELECT asset, COUNT(*), SUM(won), COALESCE(SUM(usd),0)
                 FROM fills WHERE settled_outcome IS NOT NULL GROUP BY asset""")
    per_asset = c.fetchall()
    # fills-per-window (stacking) derived live
    c.execute("SELECT COUNT(*) FROM (SELECT 1 FROM fills GROUP BY asset, tf, window_open)")
    n_windows = c.fetchone()[0] or 0
    conn.close()

    if not n_all:
        return "🔭 <b>TRADER TRACKER</b>\nNo fills captured yet — still polling."

    L = [f"🔭 <b>TRADER EDGE · {TRADER_WALLET[:10]}…</b>",
         f"Fills: <b>{n_all}</b> · windows {n_windows} · vol ${vol_all:,.0f}"]
    if n_windows:
        L.append(f"Avg {n_all/n_windows:.1f} fills/window (his stacking)")
    if n_set:
        L.append(f"Settled: {n_set} · <b>win {(n_won or 0)/n_set*100:.1f}%</b>")

    if settled:
        L.append("\n<b>Secs left → win%</b>")
        g = {}
        for s, w in settled:
            k = _bandkey(SECS_BANDS, s)
            if k:
                cell = g.setdefault(k, [0, 0]); cell[0] += 1; cell[1] += int(w or 0)
        for k in SECS_BANDS:
            if k in g:
                n, won = g[k]
                lbl = "≤3s(buzzer)" if k == (-5, 3) else f"{k[0]}-{k[1]}s"
                L.append(f"  {lbl}: {won/n*100:.1f}% ({n})")

    if with_move:
        L.append("\n<b>|Move| at entry → win%  ← the gate</b>")
        g = {}
        for m, w in with_move:
            k = _bandkey(MOVE_BANDS, m)
            if k:
                cell = g.setdefault(k, [0, 0]); cell[0] += 1; cell[1] += int(w or 0)
        for (lo, hi) in MOVE_BANDS:
            if (lo, hi) in g:
                n, won = g[(lo, hi)]
                L.append(f"  {lo:.2f}-{hi if hi<900 else '∞'}%: {won/n*100:.1f}% ({n})")
    else:
        L.append("\n<i>(move column still warming up — needs ~25 min of tick history)</i>")

    if per_asset:
        L.append("\n<b>By asset</b>")
        for a, n, won, usd in sorted(per_asset, key=lambda x: -x[3]):
            L.append(f"  {ASSET_EMOJI.get(a,'')}{a}: {(won or 0)/n*100:.0f}% ({n}) ${usd:,.0f}")

    L.append(f"\n🕐 {est_str()}")
    return "\n".join(L)

def build_markets_report():
    """Which markets he enters and when — both the 5m-vs-15m strategic split and
    the per-asset × per-timeframe grid. Timing is reliable for his early entries;
    buzzer-level numbers are softened by settlement-timestamp batching."""
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    L = ["🗺️ <b>HIS MARKETS — where & when</b>", ""]

    # --- 5m vs 15m strategic split ---
    L.append("<b>By timeframe</b>")
    c.execute("""SELECT tf, COUNT(*), COALESCE(SUM(usd),0),
                        AVG(secs_left),
                        AVG(CASE WHEN binance_move_window IS NOT NULL
                                 THEN ABS(binance_move_window) END),
                        AVG(CASE WHEN settled_outcome IS NOT NULL THEN CAST(won AS REAL) END)
                 FROM fills GROUP BY tf ORDER BY tf""")
    for tf, n, usd, avg_sl, avg_mv, wr in c.fetchall():
        mvs = f"{avg_mv:.3f}%" if avg_mv is not None else "—"
        wrs = f"{wr*100:.1f}%" if wr is not None else "—"
        sls = f"{avg_sl:.0f}s" if avg_sl is not None else "—"
        L.append(f"  <b>{tf}m</b>: {n} fills · ${usd:,.0f} · enter ~{sls} left · "
                 f"move {mvs} · win {wrs}")
    L.append("")

    # --- per-asset × per-timeframe grid ---
    L.append("<b>By market (asset × tf)</b>")
    L.append("<i>fills · vol · avg secs-left · win%</i>")
    c.execute("""SELECT asset, tf, COUNT(*), COALESCE(SUM(usd),0), AVG(secs_left),
                        AVG(CASE WHEN settled_outcome IS NOT NULL THEN CAST(won AS REAL) END)
                 FROM fills GROUP BY asset, tf ORDER BY SUM(usd) DESC""")
    rows = c.fetchall()
    conn.close()
    for asset, tf, n, usd, avg_sl, wr in rows:
        em = ASSET_EMOJI.get(asset, "")
        sls = f"{avg_sl:.0f}s" if avg_sl is not None else "—"
        wrs = f"{wr*100:.0f}%" if wr is not None else "—"
        L.append(f"  {em}{asset} {tf}m: {n} · ${usd:,.0f} · {sls} · {wrs}")
    if not rows:
        L.append("  no fills captured yet")
    L.append(f"\n🕐 {est_str()}")
    return "\n".join(L)


def build_recent(n=12):
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("""SELECT asset, tf, outcome, price, size, secs_left,
                        binance_move_window, settled_outcome, won
                 FROM fills ORDER BY fill_ts DESC LIMIT ?""", (n,))
    rows = c.fetchall(); conn.close()
    if not rows:
        return "No fills yet."
    out = ["🧾 <b>His recent fills</b>"]
    for a, tf, oc, pr, sz, sl, mv, so, won in rows:
        arrow = "🔺" if oc == "UP" else "🔻"
        res = "✅" if won == 1 else "❌" if won == 0 else "⏳"
        mvs = f"{mv:+.3f}%" if mv is not None else "move?"
        out.append(f"{ASSET_EMOJI.get(a,'')}{a} {tf}m {arrow} {sz:g}@{pr*100:.0f}¢ "
                   f"· {sl}s left · {mvs} {res}")
    return "\n".join(out)

def build_trade_lines(since_ts, max_lines=60):
    """Individual fills with fill_ts > since_ts: entry time (secs left), move, price,
    size, result. Newest first. Capped so the message stays under Telegram limits."""
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("""SELECT fill_ts, asset, tf, outcome, secs_left, binance_move_window,
                        price, size, won
                 FROM fills WHERE fill_ts > ? ORDER BY fill_ts DESC LIMIT ?""",
              (since_ts, max_lines + 1))
    rows = c.fetchall()
    c.execute("SELECT COUNT(*) FROM fills WHERE fill_ts > ?", (since_ts,))
    total_new = c.fetchone()[0] or 0
    conn.close()
    if not rows:
        return f"\n<b>Trades since last report:</b> none"
    out = [f"\n<b>Trades since last report ({total_new})</b>",
           "<i>asset · dir · secs-left · move · px · size · result</i>"]
    for fts, a, tf, oc, sl, mv, pr, sz, won in rows[:max_lines]:
        t = (datetime.fromtimestamp(fts, timezone.utc) - timedelta(hours=4)).strftime("%H:%M:%S")
        arrow = "🔺" if oc == "UP" else "🔻"
        res = "✅" if won == 1 else "❌" if won == 0 else "⏳"
        mvs = f"{mv:+.3f}%" if mv is not None else "—"
        out.append(f"{t} {ASSET_EMOJI.get(a,'')}{a}{tf} {arrow} {sl}s {mvs} "
                   f"{pr*100:.0f}¢ {sz:g} {res}")
    if total_new > max_lines:
        out.append(f"…+{total_new - max_lines} more (see DB / /recent)")
    return "\n".join(out)


def report_worker():
    last_report_ts = int(time.time())
    while True:
        time.sleep(REPORT_SECS)
        try:
            summary = build_report()
            trades = build_trade_lines(last_report_ts)
            last_report_ts = int(time.time())
            msg = summary + "\n" + trades
            # Telegram hard-caps ~4096 chars; split if needed.
            if len(msg) <= 3800:
                tg(msg)
            else:
                tg(summary)
                tg(trades)
        except Exception as e:
            log.error(f"[report] {e}")

# ─── COMMANDS ────────────────────────────────────────────────────────────────
def command_worker():
    global _tg_offset
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    while True:
        try:
            params = {"timeout": 20, "allowed_updates": ["message"]}
            if _tg_offset:
                params["offset"] = _tg_offset
            res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                               params=params, timeout=25)
            for upd in res.json().get("result", []):
                _tg_offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                if str(msg.get("chat", {}).get("id", "")) != str(TELEGRAM_CHAT_ID):
                    continue
                text = (msg.get("text", "") or "").strip().lower()
                if text == "/report":
                    tg(build_report())
                elif text == "/markets":
                    tg(build_markets_report())
                elif text == "/recent":
                    tg(build_recent())
                elif text == "/status":
                    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                    c.execute("SELECT COUNT(*), SUM(CASE WHEN settled_outcome IS NULL THEN 1 ELSE 0 END), "
                              "SUM(CASE WHEN binance_move_window IS NOT NULL THEN 1 ELSE 0 END) FROM fills")
                    tot, pend, withmv = c.fetchone(); conn.close()
                    tg(f"📡 <b>Capture status</b>\nFills: {tot or 0}\n"
                       f"Pending settle: {pend or 0}\nWith move data: {withmv or 0}\n"
                       f"Book capture: {'on' if CAPTURE_BOOK else 'off'}\n🕐 {est_str()}")
        except Exception as e:
            log.warning(f"[cmd] {e}")
            time.sleep(3)

# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client missing — underlying reconstruction disabled.")
    init_db()
    if WEBSOCKET_AVAILABLE:
        threading.Thread(target=_binance_loop,
                         args=(BINANCE_WS_URL, BINANCE_SYMBOL_TO_ASSET, "Binance Spot"),
                         daemon=True).start()
        threading.Thread(target=_binance_loop,
                         args=(BINANCE_FUTURES_WS_URL, BINANCE_FUTURES_SYMBOL_TO_ASSET, "Binance Futures"),
                         daemon=True).start()
        for a in ALL_ASSETS:
            threading.Thread(target=chainlink_worker, args=(a,), daemon=True).start()
    threading.Thread(target=poll_worker, daemon=True).start()
    threading.Thread(target=settle_worker, daemon=True).start()
    threading.Thread(target=report_worker, daemon=True).start()
    threading.Thread(target=command_worker, daemon=True).start()

    tg("🔭 <b>TRADER TRACKER v2 started</b> (measure-only — places NO orders)\n\n"
       f"Target: <code>{TRADER_WALLET}</code>\n"
       f"Polling every {POLL_SECS:g}s · report every {REPORT_SECS//60} min\n"
       f"Book capture: {'on' if CAPTURE_BOOK else 'off'}\n"
       "Commands: /report /recent /status\n"
       f"🕐 {est_str()}\n\n"
       "<i>Move/vol columns fill in after ~25 min (tick history must cover his windows).</i>")

    hb = int(os.environ.get("HEARTBEAT_SECS", "60"))
    last = 0
    while True:
        try:
            now = time.time()
            if hb > 0 and now - last >= hb:
                conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                c.execute("SELECT COUNT(*), SUM(CASE WHEN settled_outcome IS NULL THEN 1 ELSE 0 END) FROM fills")
                tot, pend = c.fetchone(); conn.close()
                log.info(f"[HB] fills={tot or 0} pending={pend or 0}")
                last = now
            time.sleep(1)
        except Exception as e:
            log.error(f"[main] {e}"); time.sleep(1)


if __name__ == "__main__":
    main()
