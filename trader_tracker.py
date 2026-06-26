#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# CERTAINTY PROBE v2  (measure-only — NEVER places an order)
#
# Per-second time-series of EVERY window across 7 assets × 5m/15m. For each
# window, through its final SAMPLE_WINDOW_SECS, once per second it records:
#   - secs_left
#   - binance_move    (underlying vs window open — the leading price)
#   - chainlink_move  (settlement feed vs its open — lags Binance)
#   - bin_minus_cl    (#3) Binance move minus Chainlink move = the live oracle-lag
#                     edge: how far the settlement feed is behind the real move
#   - realized_vol (30s) and flip_count (30s)
#   - secs_since_flip (#4) seconds since the leading direction last flipped
#                     (recency of last flip — a sharper certainty signal than count)
#   - poly_ask / poly_bid / poly_mid (#2) the Polymarket price trajectory
#   - poly_depth99 (#1) shares available at ≤99¢ on the favored side, i.e. the
#                     liquidity YOU would have to fill into  + book_age
# At settlement it labels each sample with the real outcome and whether the
# Binance direction was correct. Places no orders.
#
# Book data is gathered by a separate poller that keeps each active window's book
# fresh in memory, so the 1-second sampler never blocks on network.
# ─────────────────────────────────────────────────────────────────────────────

import os
import time
import json
import math
import bisect
import sqlite3
import logging
import threading
import statistics
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

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
TIMEFRAMES = [5, 15]

SAMPLE_WINDOW_SECS = int(os.environ.get("SAMPLE_WINDOW_SECS", "120"))  # final N sec sampled
SAMPLE_INTERVAL    = float(os.environ.get("SAMPLE_INTERVAL", "1.0"))
VOL_LOOKBACK_SECS  = int(os.environ.get("VOL_LOOKBACK_SECS", "30"))
CAPTURE_BOOK       = os.environ.get("CAPTURE_BOOK", "true").lower() == "true"
BOOK_SECS          = int(os.environ.get("BOOK_SECS", "75"))   # only fetch book inside final N sec
BOOK_REFRESH       = float(os.environ.get("BOOK_REFRESH", "3.0"))  # book poll cadence per market (2 sides → a bit slower)
REPORT_SECS        = int(os.environ.get("REPORT_SECS", "3600"))
DB_PATH            = os.environ.get("PROBE_DB", "certainty_probe.db")

ASSET_SLUG = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge",
              "BNB": "bnb", "XRP": "xrp", "HYPE": "hype"}
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("certainty-probe")

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

binance_hist   = {a: deque() for a in ASSET_LIST}
prices_binance = {}
binance_last   = {}
prices_chainlink      = {}
chainlink_last_update = {}
_hist_lock = threading.Lock()
TICK_RETAIN = 700

def _push_binance(asset, price):
    now = time.time()
    prices_binance[asset] = price
    binance_last[asset] = now
    with _hist_lock:
        dq = binance_hist[asset]
        dq.append((now, price))
        cutoff = now - TICK_RETAIN
        while dq and dq[0][0] < cutoff:
            dq.popleft()

def _snap(asset):
    with _hist_lock:
        return list(binance_hist[asset])

# ─── TELEGRAM ────────────────────────────────────────────────────────────────
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
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_key TEXT, asset TEXT, tf INTEGER, secs_left INTEGER,
            binance_move REAL, chainlink_move REAL, bin_minus_cl REAL,
            realized_vol REAL, flip_count INTEGER, secs_since_flip INTEGER,
            poly_ask REAL, poly_bid REAL, poly_mid REAL, poly_depth99 REAL, book_age INTEGER,
            binance_dir TEXT, settled_outcome TEXT, correct INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_win ON samples(window_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_set ON samples(settled_outcome)")
    conn.commit(); conn.close()

def db_insert(row):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""INSERT INTO samples
        (window_key, asset, tf, secs_left, binance_move, chainlink_move, bin_minus_cl,
         realized_vol, flip_count, secs_since_flip,
         poly_ask, poly_bid, poly_mid, poly_depth99, book_age,
         binance_dir, settled_outcome, correct)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)""",
        (row["window_key"], row["asset"], row["tf"], row["secs_left"],
         row["binance_move"], row["chainlink_move"], row["bin_minus_cl"],
         row["realized_vol"], row["flip_count"], row["secs_since_flip"],
         row["poly_ask"], row["poly_bid"], row["poly_mid"], row["poly_depth99"], row["book_age"],
         row["binance_dir"]))
    conn.commit(); conn.close()

def db_label(window_key, outcome):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""UPDATE samples SET settled_outcome=?,
                    correct=CASE WHEN binance_dir=? THEN 1 ELSE 0 END
                    WHERE window_key=?""", (outcome, outcome, window_key))
    conn.commit(); conn.close()

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
                        _push_binance(a, p)
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
    reconnect = float(os.environ.get("CL_RECONNECT_SECS", "5.0"))
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
                        prices_chainlink[asset] = v
                        chainlink_last_update[asset] = time.time()
                        break
                except websocket.WebSocketTimeoutException:
                    break
                except Exception:
                    break
        except Exception as e:
            log.warning(f"[Chainlink] {asset} {e}")
            # On rate-limit (429) handshake rejections, wait longer before retrying.
            if "429" in str(e) or "Too Many Requests" in str(e):
                time.sleep(20)
        finally:
            try: ws and ws.close()
            except Exception: pass
        time.sleep(reconnect)

# ─── WINDOW MATH ─────────────────────────────────────────────────────────────
def get_window_times(tf):
    now = datetime.now(timezone.utc)
    slot = (now.minute // tf) * tf
    open_ = now.replace(minute=slot, second=0, microsecond=0)
    cm = slot + tf
    if cm >= 60:
        close_ = open_.replace(hour=(open_.hour + 1) % 24, minute=0)
    else:
        close_ = open_.replace(minute=cm)
    return open_, close_

def wkey(asset, tf, open_time):
    return f"{asset}_{tf}_{open_time.strftime('%Y%m%d%H%M')}"

def price_at(ticks, ts, trust=15):
    if not ticks:
        return None
    times = [t for (t, _) in ticks]
    i = bisect.bisect_left(times, ts)
    cands = []
    if i < len(ticks): cands.append(ticks[i])
    if i > 0: cands.append(ticks[i-1])
    if not cands: return None
    best = min(cands, key=lambda tp: abs(tp[0]-ts))
    return best[1] if abs(best[0]-ts) <= trust else None

def realized_vol(asset, lookback=VOL_LOOKBACK_SECS):
    ticks = _snap(asset)
    if len(ticks) < 3: return 0.0
    cut = time.time() - lookback
    pts = [(t,p) for (t,p) in ticks if t >= cut]
    if len(pts) < 3: return 0.0
    rets = []
    for i in range(1, len(pts)):
        p0 = pts[i-1][1]
        if p0 > 0: rets.append((pts[i][1]-p0)/p0*100)
    return math.sqrt(sum(r*r for r in rets)) if rets else 0.0

def flip_count(asset, lookback=30):
    ticks = _snap(asset)
    cut = time.time() - lookback
    seg = [(t,p) for (t,p) in ticks if t >= cut]
    if len(seg) < 3: return 0
    flips, prev = 0, None
    for i in range(1, len(seg)):
        d = seg[i][1]-seg[i-1][1]
        if d == 0: continue
        cur = 1 if d > 0 else -1
        if prev is not None and cur != prev: flips += 1
        prev = cur
    return flips

# ─── MARKET TOKEN CACHE + BOOK POLLER ────────────────────────────────────────
market_cache = {}   # slug -> (up_token, down_token) or None
book_state   = {}   # window_key -> dict(ts, ask, bid, mid, depth99, side)
_book_lock = threading.Lock()

def resolve_tokens(asset, tf, open_time):
    slug = f"{ASSET_SLUG[asset]}-updown-{tf}m-{int(open_time.timestamp())}"
    if slug in market_cache:
        return market_cache[slug]
    try:
        r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=8)
        data = r.json()
        toks = None
        if data and isinstance(data, list):
            markets = data[0].get("markets", [])
            if markets:
                tids = markets[0].get("clobTokenIds")
                if isinstance(tids, str):
                    tids = json.loads(tids)
                if tids and len(tids) >= 2:
                    toks = (tids[0], tids[1])  # [0]=Up, [1]=Down
        market_cache[slug] = toks
        return toks
    except Exception as e:
        log.warning(f"[tokens] {slug} {e}")
        return None

def fetch_book(token_id):
    try:
        r = requests.get("https://clob.polymarket.com/book",
                         params={"token_id": token_id}, timeout=6)
        b = r.json()
        asks = b.get("asks", []) or []
        bids = b.get("bids", []) or []
        def _p(x): return float(x.get("price", 0))
        def _s(x): return float(x.get("size", 0))
        best_ask = min((_p(a) for a in asks if _p(a) > 0), default=None)
        best_bid = max((_p(x) for x in bids if _p(x) > 0), default=None)
        depth99 = sum(_s(a) for a in asks if 0 < _p(a) <= 0.99)
        mid = ((best_ask + best_bid) / 2) if (best_ask and best_bid) else None
        return best_ask, best_bid, mid, round(depth99, 2)
    except Exception:
        return None, None, None, None

def book_poller():
    """Keeps each active window's favored-side book fresh in memory, so the
    sampler never blocks. Only fetches inside the final BOOK_SECS."""
    if not CAPTURE_BOOK:
        return
    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            # Prune windows that are no longer current, so book_state reflects real
            # state. NOTE: books are only fetched in the final BOOK_SECS before a
            # close, so books_live is correctly 0 between closes and ~7-14 just
            # before each close — that is expected, not a fault.
            active_keys = set()
            for tf in TIMEFRAMES:
                ot, _ = get_window_times(tf)
                for asset in ASSET_LIST:
                    active_keys.add(wkey(asset, tf, ot))
            with _book_lock:
                for k in list(book_state.keys()):
                    if k not in active_keys:
                        del book_state[k]
            for tf in TIMEFRAMES:
                ot, ct = get_window_times(tf)
                secs_left = (ct - now_dt).total_seconds()
                if secs_left < 0 or secs_left > BOOK_SECS:
                    continue
                for asset in ASSET_LIST:
                    toks = resolve_tokens(asset, tf, ot)
                    if not toks:
                        continue
                    # Fetch BOTH sides' books. Don't trust token ordering to tell us
                    # which is Up vs Down — instead identify the market's FAVORITE by
                    # observed price (the side trading higher). In a locked window the
                    # favorite IS the winner, so its ask is the price you'd actually pay.
                    up_tok, down_tok = toks[0], toks[1]
                    a0, b0, m0, d0 = fetch_book(up_tok)
                    a1, b1, m1, d1 = fetch_book(down_tok)
                    mid0 = m0 if m0 is not None else (a0 or 0)
                    mid1 = m1 if m1 is not None else (a1 or 0)
                    if (mid0 or 0) <= 0 and (mid1 or 0) <= 0:
                        continue
                    if (mid0 or 0) >= (mid1 or 0):
                        ask, bid, mid, depth, fav = a0, b0, m0, d0, "side0"
                    else:
                        ask, bid, mid, depth, fav = a1, b1, m1, d1, "side1"
                    if ask is None and bid is None:
                        continue
                    wk = wkey(asset, tf, ot)
                    with _book_lock:
                        book_state[wk] = {"ts": time.time(), "ask": ask, "bid": bid,
                                          "mid": mid, "depth99": depth, "side": fav}
            time.sleep(BOOK_REFRESH)
        except Exception as e:
            log.warning(f"[book_poller] {e}")
            time.sleep(BOOK_REFRESH)

# ─── SETTLEMENT ──────────────────────────────────────────────────────────────
def fetch_outcome(asset, tf, open_time):
    slug = f"{ASSET_SLUG[asset]}-updown-{tf}m-{int(open_time.timestamp())}"
    try:
        r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=10)
        data = r.json()
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
        if up >= 0.99: return "UP"
        if down >= 0.99: return "DOWN"
        return None
    except Exception as e:
        log.warning(f"[outcome] {slug} {e}")
        return None

def settle_later(window_key, asset, tf, open_time):
    def worker():
        for _ in range(180):
            o = fetch_outcome(asset, tf, open_time)
            if o:
                db_label(window_key, o)
                return
            time.sleep(10)
    threading.Thread(target=worker, daemon=True).start()

# ─── SAMPLER ─────────────────────────────────────────────────────────────────
window_state = {}  # (asset,tf) -> dict

def sampler_loop():
    while True:
        try:
            time.sleep(SAMPLE_INTERVAL)
            now_dt = datetime.now(timezone.utc)
            for tf in TIMEFRAMES:
                ot, ct = get_window_times(tf)
                for asset in ASSET_LIST:
                    bp = prices_binance.get(asset)
                    if bp is None:
                        continue
                    key = (asset, tf)
                    wk = wkey(asset, tf, ot)
                    st = window_state.get(key)
                    if st is None or st["wk"] != wk:
                        if st is not None and st.get("n", 0) > 0 and not st.get("done"):
                            settle_later(st["wk"], asset, tf, st["ot"])
                        ticks0 = _snap(asset)
                        st = {"wk": wk, "ot": ot, "ct": ct,
                              "b_open": bp, "c_open": prices_chainlink.get(asset),
                              "last_sec": None, "n": 0, "done": False,
                              "prev_sign": None, "last_flip_ts": ot.timestamp()}
                        window_state[key] = st
                    if st["c_open"] is None and prices_chainlink.get(asset) is not None:
                        st["c_open"] = prices_chainlink.get(asset)

                    secs_left = int((ct - now_dt).total_seconds())
                    if secs_left < 0 or secs_left > SAMPLE_WINDOW_SECS:
                        continue
                    if st["last_sec"] == secs_left:
                        continue
                    st["last_sec"] = secs_left

                    b_open = st["b_open"]
                    if not b_open or b_open <= 0:
                        continue
                    b_move = (bp - b_open) / b_open * 100
                    # flip recency: sign of cumulative move since open
                    sign = 1 if b_move >= 0 else -1
                    if st["prev_sign"] is not None and sign != st["prev_sign"]:
                        st["last_flip_ts"] = time.time()
                    st["prev_sign"] = sign
                    secs_since_flip = int(time.time() - st["last_flip_ts"])

                    c_open = st["c_open"]; cp = prices_chainlink.get(asset)
                    c_move = ((cp - c_open) / c_open * 100) if (c_open and cp and c_open > 0) else None
                    bmc = (b_move - c_move) if c_move is not None else None
                    rv = realized_vol(asset)
                    fc = flip_count(asset, 30)
                    b_dir = "UP" if b_move >= 0 else "DOWN"

                    pa = pb = pm = pd = None; bage = None
                    with _book_lock:
                        bk = book_state.get(wk)
                    if bk:
                        pa, pb, pm, pd = bk["ask"], bk["bid"], bk["mid"], bk["depth99"]
                        bage = int(time.time() - bk["ts"])

                    def r(x, n=4):
                        return round(x, n) if x is not None else None

                    db_insert({
                        "window_key": wk, "asset": asset, "tf": tf, "secs_left": secs_left,
                        "binance_move": r(b_move), "chainlink_move": r(c_move), "bin_minus_cl": r(bmc),
                        "realized_vol": r(rv), "flip_count": fc, "secs_since_flip": secs_since_flip,
                        "poly_ask": r(pa), "poly_bid": r(pb), "poly_mid": r(pm),
                        "poly_depth99": r(pd, 2), "book_age": bage, "binance_dir": b_dir,
                    })
                    st["n"] += 1
        except Exception as e:
            log.error(f"[sampler] {e}")

# ─── REPORT: LOCK FRONTIER ───────────────────────────────────────────────────
SECS_BANDS = [(0, 3), (3, 6), (6, 10), (10, 20), (20, 40), (40, 70), (70, 120)]
MOVE_BANDS = [(0.0, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.40), (0.40, 999)]

def ev_per_dollar(hold):
    return hold * 0.01 - (1 - hold) * 0.99

def report_loop():
    while True:
        time.sleep(REPORT_SECS)
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            c.execute("""SELECT secs_left, ABS(binance_move), correct
                         FROM samples WHERE settled_outcome IS NOT NULL""")
            rows = c.fetchall()
            c.execute("SELECT COUNT(*) FROM samples WHERE poly_depth99 IS NOT NULL")
            withbook = c.fetchone()[0] or 0
            conn.close()
            if not rows:
                tg("📊 <b>LOCK FRONTIER</b>\nNo settled samples yet — gathering.")
                continue
            grid = {}
            for sl, m, corr in rows:
                sb = next((i for i,(lo,hi) in enumerate(SECS_BANDS) if lo<=sl<hi), None)
                mb = next((i for i,(lo,hi) in enumerate(MOVE_BANDS) if lo<=m<hi), None)
                if sb is None or mb is None: continue
                cell = grid.setdefault((sb,mb),[0,0]); cell[0]+=1; cell[1]+=int(corr or 0)
            L = []
            for sb,(slo,shi) in enumerate(SECS_BANDS):
                seg, frontier = [], None
                for mb,(mlo,mhi) in enumerate(MOVE_BANDS):
                    cell = grid.get((sb,mb))
                    if not cell or cell[0] < 20: continue
                    hold = cell[1]/cell[0]
                    seg.append(f"{mlo:.2f}-{mhi if mhi<900 else '∞'}:{hold*100:.1f}%({cell[0]})")
                    if frontier is None and hold > 0.99:
                        frontier = (mlo, hold, cell[0])
                if seg:
                    L.append(f"<b>{slo}-{shi}s left</b>")
                    L.append("  " + " · ".join(seg))
                    if frontier:
                        L.append(f"  ✅ locks ≥{frontier[0]:.2f}% (hold {frontier[1]*100:.1f}%, "
                                 f"EV ${ev_per_dollar(frontier[1]):+.4f}/$1)")
                    else:
                        L.append("  ⚠️ no band cleared 99% hold")
            tg("📊 <b>LOCK FRONTIER · cumulative</b>\n<i>hold% = sign correct; >99% beats 99¢</i>\n\n"
               + "\n".join(L) + f"\n\n🧪 {len(rows)} settled samples · {withbook} w/ book · {est_str()}")
        except Exception as e:
            log.error(f"[report] {e}")

# ─── MAIN ────────────────────────────────────────────────────────────────────
_tg_offset = None

# His wallet — used only for the on-demand /markets pull (no continuous polling here;
# the tracker does that. This is a one-shot fetch when you text /markets).
TRADER_WALLET = os.environ.get(
    "TRADER_WALLET", "0xf3ce251f9c4ae0f3940a9f32de5dd1a1d05b8bc6").strip().lower()
import re as _re
_SLUG_RE = _re.compile(r"^([a-z]+)-updown-(\d+)m-(\d+)$")

def build_markets_live():
    """One-shot pull of his recent fills from the public API, summarized by timeframe
    and by market. Lighter than the tracker (no entry-move reconstruction, no full
    history) — just 'which markets, 5m vs 15m, and when', on demand."""
    try:
        r = requests.get("https://data-api.polymarket.com/trades",
                         params={"user": TRADER_WALLET, "limit": 500, "takerOnly": "false"},
                         timeout=15)
        data = r.json()
    except Exception as e:
        return f"🗺️ <b>HIS MARKETS</b>\nfetch failed: {e}"
    if not isinstance(data, list):
        return "🗺️ <b>HIS MARKETS</b>\nunexpected API response."
    his = [rec for rec in data if str(rec.get("proxyWallet", "")).lower() == TRADER_WALLET]
    # parse to (asset, tf, secs_left, usd)
    rev = {"btc":"BTC","eth":"ETH","sol":"SOL","doge":"DOGE","bnb":"BNB","xrp":"XRP","hype":"HYPE"}
    tf_agg = {}        # tf -> [n, usd, sum_secs_left, n_with_sl]
    grid = {}          # (asset,tf) -> [n, usd, sum_sl, n_sl]
    for rec in his:
        slug = rec.get("eventSlug") or rec.get("slug") or ""
        m = _SLUG_RE.match(slug)
        if not m:
            continue
        asset = rev.get(m.group(1))
        if not asset:
            continue
        tf = int(m.group(2)); wopen = int(m.group(3)); wclose = wopen + tf*60
        try:
            price = float(rec.get("price",0)); size = float(rec.get("size",0))
            fts = int(rec.get("timestamp",0))
        except (TypeError, ValueError):
            continue
        usd = price*size; sl = wclose - fts
        ta = tf_agg.setdefault(tf, [0,0.0,0,0]); ta[0]+=1; ta[1]+=usd
        if -5 <= sl <= 1000: ta[2]+=sl; ta[3]+=1
        g = grid.setdefault((asset,tf), [0,0.0,0,0]); g[0]+=1; g[1]+=usd
        if -5 <= sl <= 1000: g[2]+=sl; g[3]+=1
    if not grid:
        return "🗺️ <b>HIS MARKETS</b>\nno recent 5m/15m fills in the latest pull."
    emoji = {"BTC":"🟠","ETH":"🔷","SOL":"🟣","DOGE":"🟡","BNB":"🟨","XRP":"⚪","HYPE":"🟢"}
    L = ["🗺️ <b>HIS MARKETS — recent pull</b>", "<i>which markets · 5m vs 15m · when</i>", ""]
    L.append("<b>By timeframe</b>")
    for tf in sorted(tf_agg):
        n,usd,ssl,nsl = tf_agg[tf]
        sls = f"~{ssl/nsl:.0f}s left" if nsl else "—"
        L.append(f"  <b>{tf}m</b>: {n} fills · ${usd:,.0f} · enter {sls}")
    L.append("")
    L.append("<b>By market</b> <i>(fills · vol · avg secs-left)</i>")
    for (asset,tf), (n,usd,ssl,nsl) in sorted(grid.items(), key=lambda x:-x[1][1]):
        sls = f"{ssl/nsl:.0f}s" if nsl else "—"
        L.append(f"  {emoji.get(asset,'')}{asset} {tf}m: {n} · ${usd:,.0f} · {sls}")
    L.append(f"\n<i>Note: seconds-left softened by settlement batching; read as pattern.</i>")
    L.append(f"🕐 {est_str()}")
    return "\n".join(L)

def build_calm_report(asset):
    """THE CALM TEST: per asset per timeframe, split settled windows by how CALM
    the market was (realized_vol = avg jumpiness, and flip_count) and show WIN%
    in each calm bucket. If win% climbs as conditions get calmer, calm predicts
    winning -> a calm filter helps, and the buckets tell you where to draw the
    line (trade buckets >=99%, skip the rest). Runs on existing data."""
    if asset not in ASSET_LIST:
        return f"Unknown asset '{asset}'. Try: {', '.join(ASSET_LIST)}"
    # Calm buckets by realized_vol (lower = calmer). Edges chosen to spread the
    # mass; the report shows counts so you can see if a bucket is too thin.
    vol_buckets = [
        ("very calm  (rv<0.02)",  0.0,   0.02),
        ("calm       (0.02-0.04)",0.02,  0.04),
        ("moderate   (0.04-0.07)",0.04,  0.07),
        ("choppy     (0.07-0.12)",0.07,  0.12),
        ("very choppy(rv>0.12)",  0.12,  9.99),
    ]
    flip_buckets = [
        ("steady (<=4 flips)",   0,  5),
        ("some   (5-9 flips)",   5,  10),
        ("choppy (10-15 flips)", 10, 16),
        ("wild   (16+ flips)",   16, 9999),
    ]
    em = ASSET_EMOJI.get(asset, "")
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    # Diagnostic: how many settled rows actually have the calm columns populated?
    diag = c.execute(
        """SELECT
             SUM(CASE WHEN settled_outcome IS NOT NULL THEN 1 ELSE 0 END),
             SUM(CASE WHEN settled_outcome IS NOT NULL AND realized_vol IS NOT NULL THEN 1 ELSE 0 END),
             SUM(CASE WHEN settled_outcome IS NOT NULL AND flip_count IS NOT NULL THEN 1 ELSE 0 END),
             AVG(CASE WHEN settled_outcome IS NOT NULL THEN realized_vol END)
           FROM samples WHERE asset=?""", (asset,)).fetchone()
    n_settled = diag[0] or 0
    n_rv = diag[1] or 0
    n_fc = diag[2] or 0
    avg_rv = diag[3]
    out = [f"🧘 <b>{em} {asset} — WIN% by market calm</b>",
           "<i>does calmer = higher win%? (rv = jumpiness)</i>",
           f"<i>settled:{n_settled} · w/ vol:{n_rv} · w/ flips:{n_fc}"
           + (f" · avg rv:{avg_rv:.4f}" if avg_rv is not None else "") + "</i>"]
    if n_rv == 0 and n_fc == 0:
        out.append("\n⚠️ The calm columns (realized_vol, flip_count) are empty for")
        out.append("settled rows — this data was gathered before they were recorded.")
        out.append("Calm data will accumulate from now on; re-run /calm in a day.")
        out.append(f"\n🕐 {est_str()}")
        conn.close()
        return "\n".join(out)
    for tf in (5, 15):
        out.append(f"\n<b>━━ {asset} {tf}m · by volatility ━━</b>")
        for label, lo, hi in vol_buckets:
            rows = c.execute(
                """SELECT correct FROM samples
                   WHERE settled_outcome IS NOT NULL AND asset=? AND tf=?
                     AND realized_vol >= ? AND realized_vol < ?""",
                (asset, tf, lo, hi)).fetchall()
            n = len(rows)
            if n == 0:
                continue
            win = sum(r[0] for r in rows) / n * 100
            flag = "✅" if win >= 99 else ("·" if win >= 97 else "⚠️")
            out.append(f" {flag} {label}: {win:.1f}% ({n})")
        out.append(f"<b>━━ {asset} {tf}m · by flips ━━</b>")
        for label, lo, hi in flip_buckets:
            rows = c.execute(
                """SELECT correct FROM samples
                   WHERE settled_outcome IS NOT NULL AND asset=? AND tf=?
                     AND flip_count >= ? AND flip_count < ?""",
                (asset, tf, lo, hi)).fetchall()
            n = len(rows)
            if n == 0:
                continue
            win = sum(r[0] for r in rows) / n * 100
            flag = "✅" if win >= 99 else ("·" if win >= 97 else "⚠️")
            out.append(f" {flag} {label}: {win:.1f}% ({n})")
    conn.close()
    out.append("\n<i>✅ = beats 99¢ · ⚠️ = loses money. If the ✅s cluster in the")
    out.append("calm/steady rows, gate the bot to only trade those conditions.</i>")
    out.append(f"\n🕐 {est_str()}")
    return "\n".join(out)


def build_grid_report(asset):
    """Per-ASSET lock frontier, split by timeframe (5m and 15m) and time-left band.
    This is the measured per-market-per-timeframe surface (replaces guessed
    thresholds). Shows hold% and sample count per move bucket; the 'locks ≥X'
    line is the smallest move that holds >=99% in that band."""
    if asset not in ASSET_LIST:
        return f"Unknown asset '{asset}'. Try one of: {', '.join(ASSET_LIST)}"
    bands = [(0,3),(3,6),(6,10),(10,20),(20,40),(40,70),(70,120),(120,99999)]
    buckets = [(0.0,0.02),(0.02,0.05),(0.05,0.10),(0.10,0.20),(0.20,0.40),(0.40,99)]
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    em = ASSET_EMOJI.get(asset, "")
    out = [f"📐 <b>{em} {asset} — frontier by timeframe</b>",
           "<i>hold% per move×time; locks = smallest move ≥99%</i>"]
    for tf in (5, 15):
        out.append(f"\n<b>━━ {asset} {tf}m ━━</b>")
        any_rows = False
        for blo, bhi in bands:
            cells = []
            lock_thr = None
            for mlo, mhi in buckets:
                rows = c.execute(
                    """SELECT correct FROM samples
                       WHERE settled_outcome IS NOT NULL AND asset=? AND tf=?
                         AND secs_left>=? AND secs_left<?
                         AND ABS(binance_move)>=? AND ABS(binance_move)<?""",
                    (asset, tf, blo, bhi, mlo, mhi)).fetchall()
                n = len(rows)
                if n == 0:
                    continue
                any_rows = True
                hold = sum(r[0] for r in rows)/n*100
                cells.append(f"{mlo:.2f}-{mhi:.2f}:{hold:.0f}%({n})")
                if lock_thr is None and hold >= 99 and n >= 20:
                    lock_thr = mlo
            if cells:
                label = f"{blo}-{bhi}s" if bhi < 99999 else f"{blo}s+"
                out.append(f" {label}: " + " · ".join(cells))
                if lock_thr is not None:
                    out.append(f"   ✅ locks ≥{lock_thr:.2f}%")
                else:
                    out.append(f"   ⚠️ nothing holds ≥99% (need bigger move/less time)")
        if not any_rows:
            out.append(" (no settled samples yet for this timeframe)")
    conn.close()
    out.append(f"\n🕐 {est_str()}")
    return "\n".join(out)


def build_split_report():
    """THE KEY TEST: at the marginal zones where the bot loses, do LOSING windows
    have higher volatility / more flips than WINNING ones? If yes, a choppiness
    filter would catch losers specifically (worth adding). If winners and losers
    look the same, no filter helps and we shouldn't add one."""
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    # Marginal zones = the ones the bot actually trades and loses in:
    #   small moves at the buzzer, and moderate moves entered early (the 47s cluster)
    zones = [
        ("small move, buzzer (0.03-0.08%, 0-20s)", 0.03, 0.08, 0, 20),
        ("small move, 20-40s (0.04-0.10%)",        0.04, 0.10, 20, 40),
        ("moderate move, early (0.08-0.20%, 40-70s)", 0.08, 0.20, 40, 70),
    ]
    L = ["🔬 <b>WINNER vs LOSER — choppiness split</b>",
         "<i>do losers have higher vol/flips at same move×time?</i>", ""]
    for label, mlo, mhi, slo, shi in zones:
        rows = c.execute(
            """SELECT correct, realized_vol, flip_count, secs_since_flip
               FROM samples
               WHERE settled_outcome IS NOT NULL
                 AND ABS(binance_move) >= ? AND ABS(binance_move) < ?
                 AND secs_left >= ? AND secs_left < ?""",
            (mlo, mhi, slo, shi)).fetchall()
        if not rows:
            L.append(f"<b>{label}</b>\n  no samples yet\n")
            continue
        wins = [r for r in rows if r[0] == 1]
        loss = [r for r in rows if r[0] == 0]
        n, nl = len(rows), len(loss)
        hold = len(wins) / n * 100 if n else 0
        def avg(rs, i):
            vals = [r[i] for r in rs if r[i] is not None]
            return sum(vals) / len(vals) if vals else 0
        wv, lv = avg(wins, 1), avg(loss, 1)     # realized_vol
        wf, lf = avg(wins, 2), avg(loss, 2)     # flip_count
        ws, ls = avg(wins, 3), avg(loss, 3)     # secs_since_flip
        L.append(f"<b>{label}</b>")
        L.append(f"  hold {hold:.1f}% · {n} samp · {nl} losses")
        L.append(f"  realized_vol: win {wv:.4f} vs <b>loss {lv:.4f}</b>")
        L.append(f"  flip_count:   win {wf:.1f} vs <b>loss {lf:.1f}</b>")
        L.append(f"  secs_since_flip: win {ws:.0f} vs loss {ls:.0f}")
        # verdict
        if nl >= 5:
            vol_gap = (lv - wv) / wv * 100 if wv else 0
            flip_gap = (lf - wf) / wf * 100 if wf else 0
            if vol_gap > 25 or flip_gap > 25:
                L.append(f"  → losers ARE choppier (vol {vol_gap:+.0f}%, flips {flip_gap:+.0f}%) — filter would help ✅")
            else:
                L.append(f"  → losers ~same as winners — filter won't separate them ✗")
        else:
            L.append(f"  → too few losses ({nl}) to judge yet")
        L.append("")
    conn.close()
    L.append("<i>If losers show clearly higher vol/flips, a choppiness filter")
    L.append("catches losers specifically. If not, don't add it.</i>")
    L.append(f"\n🕐 {est_str()}")
    return "\n".join(L)


def build_book_report():
    """The execution-gap answer: in the buckets that were both LOCKED and had time
    to act, what was the actual ask price, and was there depth at ≤99¢? This is what
    decides whether the frontier edge is reachable or already priced out."""
    # (label, secs_left lo/hi, |move| lo/hi) — the safe-AND-reachable zone
    buckets = [
        ("10-20s, 0.05-0.20%", 10, 20, 0.05, 0.20),
        ("20-40s, ≥0.10%",     20, 40, 0.10, 99),
        ("40-70s, ≥0.20%",     40, 70, 0.20, 99),
    ]
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    L = ["📕 <b>BOOK / EXECUTION CHECK</b>",
         "<i>When locked & reachable — could you buy at 99¢?</i>", ""]
    any_rows = False
    for label, slo, shi, mlo, mhi in buckets:
        c.execute("""SELECT COUNT(*),
                            AVG(poly_ask), AVG(poly_mid), AVG(poly_depth99),
                            AVG(CASE WHEN poly_ask <= 0.99 THEN 1.0 ELSE 0.0 END),
                            AVG(CASE WHEN correct=1 THEN 1.0 ELSE 0.0 END)
                     FROM samples
                     WHERE settled_outcome IS NOT NULL AND poly_ask IS NOT NULL
                       AND secs_left >= ? AND secs_left < ?
                       AND ABS(binance_move) >= ? AND ABS(binance_move) < ?""",
                  (slo, shi, mlo, mhi))
        n, avg_ask, avg_mid, avg_depth, frac_buyable, hold = c.fetchone()
        if not n:
            L.append(f"<b>{label}</b>: no book samples yet")
            continue
        any_rows = True
        L.append(f"<b>{label}</b> ({n} samples, hold {hold*100:.1f}%)")
        L.append(f"  avg ask {avg_ask*100:.1f}¢ · mid {avg_mid*100:.1f}¢")
        L.append(f"  buyable ≤99¢: {frac_buyable*100:.0f}% of the time")
        L.append(f"  avg depth ≤99¢: {avg_depth:.0f} shares")
        L.append("")
    conn.close()
    if not any_rows:
        L.append("\nNot enough book samples in the safe zone yet — let it run.")
    else:
        L.append("<i>Read: if 'buyable ≤99¢' is high AND depth is real, the edge is")
        L.append("reachable. If ask is already &gt;99¢ when locked, it's priced out.</i>")
    return "\n".join(L)

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
                if text == "/book":
                    tg(build_book_report())
                elif text == "/split":
                    tg(build_split_report())
                elif text.startswith("/grid"):
                    parts = text.split()
                    asset = parts[1].upper() if len(parts) > 1 else "BTC"
                    tg(build_grid_report(asset))
                elif text.startswith("/calm"):
                    parts = text.split()
                    asset = parts[1].upper() if len(parts) > 1 else "BTC"
                    tg(build_calm_report(asset))
                elif text == "/markets":
                    tg(build_markets_live())
                elif text == "/frontier":
                    # reuse the report body by triggering one immediately
                    try:
                        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                        c.execute("SELECT COUNT(*) FROM samples WHERE settled_outcome IS NOT NULL")
                        n = c.fetchone()[0]; conn.close()
                        tg(f"Frontier report runs hourly. {n} settled samples so far. "
                           f"(Use the latest hourly LOCK FRONTIER message.)")
                    except Exception as e:
                        tg(f"frontier: {e}")
                elif text == "/status":
                    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                    c.execute("SELECT COUNT(*), SUM(CASE WHEN settled_outcome IS NOT NULL THEN 1 ELSE 0 END), "
                              "SUM(CASE WHEN poly_ask IS NOT NULL THEN 1 ELSE 0 END) FROM samples")
                    tot, settled, withbook = c.fetchone(); conn.close()
                    tg(f"🧪 Probe status\nSamples: {tot or 0}\nSettled: {settled or 0}\n"
                       f"With book: {withbook or 0}\n🕐 {est_str()}")
        except Exception as e:
            log.warning(f"[cmd] {e}")
            time.sleep(3)


def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client missing — feeds disabled.")
    init_db()
    if WEBSOCKET_AVAILABLE:
        threading.Thread(target=_binance_loop, args=(BINANCE_WS_URL, BINANCE_SYMBOL_TO_ASSET, "Binance Spot"), daemon=True).start()
        threading.Thread(target=_binance_loop, args=(BINANCE_FUTURES_WS_URL, BINANCE_FUTURES_SYMBOL_TO_ASSET, "Binance Futures"), daemon=True).start()
        for a in ASSET_LIST:
            threading.Thread(target=chainlink_worker, args=(a,), daemon=True).start()
    threading.Thread(target=book_poller, daemon=True).start()
    threading.Thread(target=sampler_loop, daemon=True).start()
    threading.Thread(target=report_loop, daemon=True).start()
    threading.Thread(target=command_worker, daemon=True).start()

    tg("🧪 <b>CERTAINTY PROBE v2 started</b> (measure-only — NO orders)\n\n"
       f"Watching {' '.join(ASSET_LIST)} · 5m+15m, 1 sample/sec, final {SAMPLE_WINDOW_SECS}s\n"
       f"Now also: oracle-lag, flip-recency, Polymarket price + depth@99¢\n"
       f"Book capture: {'on (final '+str(BOOK_SECS)+'s)' if CAPTURE_BOOK else 'off'}\n"
       f"🕐 {est_str()}")

    hb = int(os.environ.get("HEARTBEAT_SECS", "30")); last = 0
    while True:
        try:
            now = time.time()
            if hb > 0 and now - last >= hb:
                parts = []
                for a in ASSET_LIST:
                    bp = prices_binance.get(a); cp = prices_chainlink.get(a)
                    parts.append(f"{a}:B{('%g'%bp) if bp else '--'}/C{('%g'%cp) if cp else '--'}")
                with _book_lock:
                    nbk = len(book_state)
                log.info("[HB] " + "  ".join(parts) + f"  books_live={nbk}")
                last = now
            time.sleep(1)
        except Exception as e:
            log.error(f"[main] {e}"); time.sleep(1)


if __name__ == "__main__":
    main()
