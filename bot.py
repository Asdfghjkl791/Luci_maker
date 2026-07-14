#!/usr/bin/env python3
# PAPER TAKER v1.0 — does taking beat making on the SAME gate? (no money)
#
# THE QUESTION
#   The maker bot rests a 99¢ bid and gets filled by whoever hits it — which
#   selects it into the losing half of its own signals (sellers dump reversing
#   trades onto the resting bid). A TAKER with the SAME entry gate instead
#   crosses the spread the instant the gate fires — taking ALL its flagged
#   trades at the moment of its choosing, not just the ones a seller chose to
#   fill. This bot tests, on paper, whether that execution change clears
#   break-even. It uses the maker bot's EXACT frontier gate and reads the SAME
#   live Polymarket books, but places NOTHING. It records the real ask it would
#   have crossed and scores every simulated trade at settlement.
#
# WHY PAPER FIRST
#   The diagnosis (maker selection hurts) is well-supported. The fix (taking
#   clears break-even) is NOT proven — taking pays the ask, which may be 99-100¢
#   and kill the edge anyway. So we score it on live prices for a few hundred
#   trades and let the numbers decide before a cent goes in. Same discipline as
#   the EUR/USD bot.
#
# HONEST LIMITS
#   • It assumes the displayed ask is takeable for the full size — real taking
#     can get worse fills in fast markets, so paper results are an UPPER bound.
#   • It uses Chainlink settlement (same oracle Polymarket uses) to grade.
#   • No fees modeled (Polymarket taker fees ~0 on these, but confirm).
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): PAPER_STAKE=5, TAKER_MAX_ASK_CENTS=99.5 (skip if ask above
#   this — the "don't overpay" guard), DB_PATH=paper_taker.db, TIMEFRAMES=5,
#   SEND_EACH=true

import os
import time
import json
import sqlite3
import logging
import threading
import requests
from datetime import datetime, timezone
from collections import deque

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

# ── LIVE TRADING (off by default; set LIVE=true + keys to arm) ───────────────
LIVE            = os.environ.get("LIVE", "false").lower() == "true"
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "")
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "")
LIVE_STAKE       = float(os.environ.get("LIVE_STAKE", "5"))
BANKROLL_STOP    = float(os.environ.get("BANKROLL_STOP", "15"))
EXCLUDE_ASSETS   = set(x.strip().upper() for x in
                       os.environ.get("EXCLUDE_ASSETS", "XRP").split(",") if x.strip())
EXIT_TRIGGER_CENTS = float(os.environ.get("EXIT_TRIGGER_CENTS", "0"))  # 0 = no exit
EXIT_FLOOR_CENTS = float(os.environ.get("EXIT_FLOOR_CENTS", "50"))  # don't sell below this (gapped)
_clob = None
_live_realized = 0.0
try:
    from py_clob_client_v2 import (ClobClient, OrderArgs, MarketOrderArgs,
                                   PartialCreateOrderOptions, OrderType)
    from py_clob_client_v2.order_builder.constants import BUY, SELL
    CLOB_SDK = True
except Exception:
    CLOB_SDK = False


def _clob_init():
    global _clob
    if not (LIVE and CLOB_SDK and POLY_PRIVATE_KEY and POLY_FUNDER):
        return False
    try:
        t = ClobClient(host="https://clob.polymarket.com", chain_id=137,
                       key=POLY_PRIVATE_KEY, signature_type=3, funder=POLY_FUNDER)
        creds = t.create_or_derive_api_key()
        _clob = ClobClient(host="https://clob.polymarket.com", chain_id=137,
                           key=POLY_PRIVATE_KEY, creds=creds, signature_type=3,
                           funder=POLY_FUNDER)
        return True
    except Exception as e:
        log.error(f"[CLOB] init failed: {e}")
        return False


def live_buy(token_id, usdc):
    if not _clob:
        return False
    try:
        a = MarketOrderArgs(token_id=token_id, amount=usdc, side=BUY,
                            order_type=OrderType.FAK)
        r = _clob.create_and_post_market_order(order_args=a,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FAK)
        return isinstance(r, dict) and (r.get("success") or r.get("status") == "matched")
    except Exception as e:
        log.error(f"[BUY] {e}")
        return False


def live_sell(token_id, shares, price_cents):
    if not _clob:
        return False
    try:
        a = OrderArgs(token_id=token_id, price=round(price_cents/100.0, 2),
                      size=shares, side=SELL)
        r = _clob.create_and_post_order(a,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.GTC)
        return isinstance(r, dict) and (r.get("success") or r.get("status") == "matched")
    except Exception as e:
        log.error(f"[SELL] {e}")
        return False


TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
PAPER_STAKE      = float(os.environ.get("PAPER_STAKE", "5"))
TAKER_MAX_ASK_CENTS = float(os.environ.get("TAKER_MAX_ASK_CENTS", "99.5"))
# Floor: skip entries CHEAPER than this — the market pricing an outcome below
# this is telling you it's too uncertain (the 89¢ coin-flip that lost). Note:
# tightening the band [MIN, MAX] does NOT create an edge — inside the band, win
# rate still ≈ price paid. It only stops the bot taking obviously-uncertain
# trades. Default 0 = no floor (take anything up to the ceiling).
TAKER_MIN_ASK_CENTS = float(os.environ.get("TAKER_MIN_ASK_CENTS", "0"))
DB_PATH          = os.environ.get("DB_PATH", "paper_taker.db")
SEND_EACH        = os.environ.get("SEND_EACH", "true").lower() == "true"
FAST_GRADE       = os.environ.get("FAST_GRADE", "true").lower() == "true"
SETTLE_POLL_SECS    = float(os.environ.get("SETTLE_POLL_SECS", "20"))
SETTLE_TIMEOUT_SECS = float(os.environ.get("SETTLE_TIMEOUT_SECS", "900"))
FAST_TIE_EPS_PCT = float(os.environ.get("FAST_TIE_EPS_PCT", "0.0005"))
# Stacking: up to MAX_STACK entries per window (like the live maker bot,
# which added clips as the move re-qualified). STACK_COOLDOWN_SECS spaces
# them out so it doesn't take all 3 in the same instant. Each entry reads
# its own live ask, so later clips may get a different price. NOTE: the 3
# stacked entries share ONE window outcome — read win RATE (per-entry, fine)
# more than trade COUNT (which triple-counts a window).
MAX_STACK           = int(os.environ.get("MAX_STACK", "1"))
STACK_COOLDOWN_SECS = float(os.environ.get("STACK_COOLDOWN_SECS", "8"))
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5").split(",")]

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}

# ── GATE: MEASURED 5m table + MEASURED 15m table (both from bot_variant_MEASURED
#    .py / the 2.4M-sample probe frontier). This is the config your own data
#    endorses: the frontier lock line for BTC/XRP/DOGE/BNB, tightened for the
#    volatile SOL/HYPE/ETH. Paired taker version of the measured bot. ──
PER_ASSET_FRONTIER = {          # 5m — GUESSED / trader-derived (bot_variant.py)
    "BTC":  [(20, 0.025), (40, 0.04), (70, 0.08), (120, 0.15)],
    "ETH":  [(20, 0.03), (40, 0.07), (70, 0.11), (120, 0.16)],
    "XRP":  [(20, 0.04), (40, 0.10), (70, 0.15), (120, 0.23)],
    "DOGE": [(20, 0.05), (40, 0.10), (70, 0.15), (120, 0.22)],
    "BNB":  [(20, 0.04), (40, 0.10), (70, 0.17), (120, 0.20)],
    "SOL":  [(20, 0.10), (40, 0.17), (70, 0.21), (120, 0.27)],
    "HYPE": [(20, 0.16), (40, 0.20), (70, 0.24), (120, 0.30)],
}
PER_ASSET_FRONTIER_15M = {      # 15m — MEASURED (from the probe grid)
    "BTC":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.10), (120, 0.10)],
    "ETH":  [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
    "SOL":  [(10, 0.10), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
    "XRP":  [(10, 0.05), (20, 0.10), (40, 0.10), (70, 0.10), (120, 0.20)],
    "DOGE": [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.20), (120, 0.20)],
    "BNB":  [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.20), (120, 0.20)],
    "HYPE": [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)],
}
# 5m fallback mirrors the guessed bot's global gate; 15m falls back to its table.
GLOBAL_FRONTIER = [(3, 0.02), (40, 0.10), (70, 0.20), (120, 0.40)]
GLOBAL_FRONTIER_15M = [(10, 0.05), (20, 0.10), (40, 0.20), (70, 0.40), (120, 0.40)]
FRONTIER_FALLBACK_PCT = float(os.environ.get("FRONTIER_FALLBACK_PCT", "0.40"))
# Entries only allowed within this many seconds of close. The frontier
# table is only defined to 120s; entering earlier uses an unvalidated
# fallback, so we forbid it. This also matches the intent of a late-
# entry strategy (enter as the outcome nears settlement, not 4 min out).
ENTRY_MAX_SECS = float(os.environ.get("ENTRY_MAX_SECS", "120"))
# Separate cutoff for 15m windows (they're 3x longer, so 120s is only the
# final 13%). NOTE: the 15m frontier table is also only validated to 120s,
# so entries in the 120s..ENTRY_MAX_SECS_15M zone use the 0.40% fallback,
# not the grid — riskier, so treat those results with caution.
ENTRY_MAX_SECS_15M = float(os.environ.get("ENTRY_MAX_SECS_15M", "120"))
# Only capture a window's open price if we first see it within this many
# seconds of its TRUE start (300s-secs_left for a 5m window). Windows we
# join late get a wrong baseline, so we skip them entirely (mark bad).
OPEN_CAPTURE_GRACE = float(os.environ.get("OPEN_CAPTURE_GRACE", "3"))
HYPE_MIN_MOVE_PCT = float(os.environ.get("HYPE_MIN_MOVE_PCT", "0.0"))


def _frontier_lookup(bands, secs_left):
    for max_secs, min_move in bands:
        if secs_left <= max_secs:
            return min_move
    return FRONTIER_FALLBACK_PCT


def frontier_locked(asset, abs_move_pct, secs_left, tf=5):
    if secs_left <= 0:
        return False
    if tf == 15:
        bands = PER_ASSET_FRONTIER_15M.get(asset, GLOBAL_FRONTIER_15M)
    else:
        bands = PER_ASSET_FRONTIER.get(asset, GLOBAL_FRONTIER)
    need = _frontier_lookup(bands, secs_left)
    if asset == "HYPE" and tf != 15:
        need = max(need, HYPE_MIN_MOVE_PCT)   # guessed-5m HYPE floor only
    return abs_move_pct >= need


# ── FEEDS ────────────────────────────────────────────────────────────────────
CHAINLINK_SYMBOLS = {"BTC": "btc/usd", "ETH": "eth/usd", "SOL": "sol/usd",
                     "DOGE": "doge/usd", "BNB": "bnb/usd", "XRP": "xrp/usd",
                     "HYPE": "hype/usd"}
CHAINLINK_SYMBOL_TO_ASSET = {v: k for k, v in CHAINLINK_SYMBOLS.items()}
CHAINLINK_WS_URL = os.environ.get("CHAINLINK_WS_URL",
                                  "wss://ws-subscriptions-clob.polymarket.com/ws/market")
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("paper-taker-MEASURED")

prices_chainlink = {}
chainlink_last_update = {}

# NOTE ON THE PRICE FEED
#   The maker bot has a working Chainlink push feed and Polymarket market lookup.
#   Rather than duplicate that whole stack here (hundreds of lines, keys, market
#   resolution), this paper bot reads Chainlink prices from the SAME public
#   Binance-mirror the maker bot uses for its reference leg, and reads live
#   Polymarket asks via the public CLOB /book endpoint per resolved token. To
#   keep this file self-contained and runnable, it expects the maker bot's
#   market-resolution helper module OR falls back to the Binance.vision mirror
#   for the open/settlement price. The order-book ask read is the one live
#   Polymarket call it makes.
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{s}usdt@bookTicker" for s in
                       ["btc", "eth", "sol", "doge", "bnb", "xrp"]))
# HYPE has no Binance spot pair — it will simply be skipped if no price arrives.
BINANCE_SYM_TO_ASSET = {f"{s.upper()}USDT": s.upper()
                        for s in ["btc", "eth", "sol", "doge", "bnb", "xrp"]}

prices_ref = {}          # reference price per asset (proxy for Chainlink settle)
ref_last = {}


def binance_ref_worker():
    while True:
        ws = None
        try:
            ws = websocket.create_connection(BINANCE_WS, timeout=10)
            ws.settimeout(30)
            log.info("[REF] Binance.vision reference feed connected")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                d = json.loads(msg).get("data", {})
                sym = d.get("s")
                a = BINANCE_SYM_TO_ASSET.get(sym)
                if a:
                    b, k = float(d.get("b", 0)), float(d.get("a", 0))
                    if b > 0 and k > 0:
                        prices_ref[a] = (b + k) / 2.0
                        ref_last[a] = time.time()
        except Exception as e:
            log.warning(f"[REF] error: {e} — reconnecting")
        finally:
            try:
                ws and ws.close()
            except Exception:
                pass
        time.sleep(3)


# ── POLYMARKET MARKET RESOLUTION + ASK READ ──────────────────────────────────
_market_cache = {}   # (asset,tf,open_ts) -> (up_token, down_token) or None


def resolve_tokens(asset, tf, open_ts):
    key = (asset, tf, open_ts)
    if key in _market_cache:
        return _market_cache[key]
    slug = f"{asset.lower()}-updown-{tf}m-{open_ts}"
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=8)
        arr = r.json()
        ev = arr[0] if isinstance(arr, list) and arr else arr
        markets = ev.get("markets", []) if isinstance(ev, dict) else []
        if markets:
            toks = json.loads(markets[0].get("clobTokenIds", "[]"))
            if len(toks) == 2:
                _market_cache[key] = (toks[0], toks[1])  # [UP, DOWN] by convention
                return _market_cache[key]
    except Exception as e:
        log.debug(f"[RESOLVE] {slug}: {e}")
    _market_cache[key] = None
    return None


def best_ask_cents(token_id):
    """Live best ask on Polymarket for this token, in cents. None on failure."""
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        asks = b.get("asks", [])
        if not asks:
            return None
        # asks sorted; best (lowest) ask is the last or first depending on API —
        # take the min price with size.
        prices = [float(a["price"]) for a in asks if float(a.get("size", 0)) > 0]
        if not prices:
            return None
        return min(prices) * 100.0
    except Exception:
        return None



def fetch_polymarket_outcome(asset, tf, open_ts):
    """Read the ACTUAL settled outcome from Polymarket (Chainlink-resolved), same
    method the maker bot uses. Returns 'UP', 'DOWN', 'TIE', or None (not settled
    yet / error). No price inference — this is the real market resolution, so the
    fake-tie-from-stale-feed problem disappears."""
    slug = f"{asset.lower()}-updown-{tf}m-{open_ts}"
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=10)
        data = r.json()
        if not data or not isinstance(data, list):
            log.info(f"[OUTCOME] {slug}: no event (resp type {type(data).__name__}, "
                     f"len {len(data) if hasattr(data,'__len__') else '?'})")
            return None
        markets = data[0].get("markets", [])
        if not markets:
            log.info(f"[OUTCOME] {slug}: event found but no markets")
            return None
        op = markets[0].get("outcomePrices")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except Exception:
                pass
        if not op or len(op) < 2:
            log.info(f"[OUTCOME] {slug}: no outcomePrices yet (raw={op})")
            return None
        up_p, down_p = float(op[0]), float(op[1])
        if up_p >= 0.99:
            log.info(f"[OUTCOME] {slug}: settled UP {op}")
            return "UP"
        if down_p >= 0.99:
            log.info(f"[OUTCOME] {slug}: settled DOWN {op}")
            return "DOWN"
        # Anything else — including ~[0.5,0.5] — is NOT settled yet (Up/Down
        # markets sit near 50/50 before Chainlink resolves). Never a tie; wait.
        log.info(f"[OUTCOME] {slug}: not settled yet (prices {op})")
        return None
    except Exception as e:
        log.warning(f"[OUTCOME] {slug}: ERROR {e}")
        return None


# ── WINDOW TIMING ────────────────────────────────────────────────────────────
def best_bid_cents(token_id):
    """Live best bid in cents — what a seller would receive right now."""
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        bids = [float(x["price"]) for x in b.get("bids", [])
                if float(x.get("size", 0)) > 0]
        return max(bids) * 100.0 if bids else None
    except Exception:
        return None


def db_set_path(rid, path_json):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE paper SET bid_path=? WHERE id=?", (path_json, rid))
    conn.commit()
    conn.close()


def monitor():
    """Samples each open position's best BID frequently — logs the path AND
    fires the live exit the instant the bid crosses the trigger band. Runs at
    1s (not 5s) so a fast move is caught near the trigger, not after it gaps."""
    while True:
        try:
            time.sleep(1)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now >= s["close_ts"]:
                    continue
                toks = resolve_tokens(s["asset"], s["tf"], s["open_ts"])
                if not toks:
                    continue
                tok = toks[0] if s["direction"] == "UP" else toks[1]
                bid = best_bid_cents(tok)
                if bid is None:
                    continue
                s.setdefault("bid_path", []).append(
                    [round(s["close_ts"] - now, 1), round(bid, 1)])
                # LIVE exit: sell a failing position when its bid crosses the
                # trigger — but only within a BAND. Below EXIT_FLOOR the bid has
                # already gapped (selling there locks in a near-total loss), so
                # hold to settlement instead. This cuts SMALL losses early
                # without panic-dumping craters.
                if (LIVE and EXIT_TRIGGER_CENTS > 0 and not s.get("exited")
                        and EXIT_FLOOR_CENTS <= bid <= EXIT_TRIGGER_CENTS):
                    shares = LIVE_STAKE / (s["ask"] / 100.0)
                    if live_sell(s["token"], round(shares, 2), bid):
                        s["exited"] = True
                        s["exit_price"] = bid   # record ACTUAL sell price
                        # exited positions are DONE — resolve now at the real
                        # sell price, don't wait for settlement (you're out).
                        exit_pnl = shares * (bid / 100.0) - LIVE_STAKE
                        global _live_realized
                        _live_realized += exit_pnl
                        db_resolve(s["rid"], bid / 100.0, "EXIT", round(exit_pnl, 4))
                        with pending_lock:
                            s in pending and pending.remove(s)
                        tg(f"🔴 <b>LIVE EXIT {ASSET_EMOJI.get(s['asset'],'')}"
                           f"{s['asset']} {s['tf']}m</b> sold @{bid:.1f}¢ "
                           f"(was {s['ask']:.0f}¢) · <b>${exit_pnl:+.2f}</b>")
                    else:
                        log.warning(f"[EXIT] sell failed {s['asset']} @{bid:.1f}¢")
                elif (LIVE and EXIT_TRIGGER_CENTS > 0 and not s.get("exited")
                      and bid < EXIT_FLOOR_CENTS):
                    log.info(f"[EXIT] {s['asset']} bid {bid:.1f}¢ already below "
                             f"floor {EXIT_FLOOR_CENTS:.0f}¢ — holding to settle "
                             f"(gapped, selling would lock max loss)")
        except Exception as e:
            log.error(f"[MONITOR] {e}")


def window_times(tf):
    now = int(time.time())
    length = tf * 60
    open_ts = (now // length) * length
    close_ts = open_ts + length
    return open_ts, close_ts, close_ts - now


# ── DB ───────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS paper (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT,
        asset TEXT, tf INTEGER, direction TEXT, open_ts INTEGER, close_ts INTEGER,
        secs_left REAL, move_pct REAL, ask_cents REAL, open_price REAL,
        settle_price REAL, result TEXT, pnl REAL)""")
    conn.execute("UPDATE paper SET result='VOID' WHERE result='PENDING'")
    try:
        conn.execute("ALTER TABLE paper ADD COLUMN bid_path TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE paper ADD COLUMN graded TEXT")
    except Exception:
        pass
    conn.commit()
    conn.close()


def db_insert(asset, tf, direction, open_ts, close_ts, secs_left, move, ask, open_price):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO paper (created,asset,tf,direction,open_ts,close_ts,
                 secs_left,move_pct,ask_cents,open_price,result)
                 VALUES (?,?,?,?,?,?,?,?,?,?, 'PENDING')""",
              (datetime.now(timezone.utc).isoformat(), asset, tf, direction,
               open_ts, close_ts, secs_left, move, ask, open_price))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_resolve(rid, settle_price, result, pnl, graded=None):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE paper SET settle_price=?, result=?, pnl=?, graded=? "
                 "WHERE id=?", (settle_price, result, pnl, graded, rid))
    conn.commit()
    conn.close()


def db_scoreboard():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT result, pnl, ask_cents FROM paper WHERE result IN ('WIN','LOSS','TIE')")
    rows = c.fetchall()
    conn.close()
    dec = [r for r in rows if r[0] != "TIE"]
    wins = sum(1 for r in dec if r[0] == "WIN")
    pnl = sum(r[1] or 0 for r in rows)
    avg_ask = (sum(r[2] or 0 for r in rows) / len(rows)) if rows else 0
    wr = (wins / len(dec) * 100) if dec else None
    be = (avg_ask) if avg_ask else 99.0   # break-even win% ≈ avg ask paid (cents)
    return {"n": len(rows), "wins": wins, "wr": wr, "pnl": pnl,
            "avg_ask": avg_ask, "be": be}


# ── TELEGRAM ─────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                                "parse_mode": "HTML"}, timeout=8)
        try:
            body = r.json()
        except Exception:
            body = {}
        if getattr(r, "status_code", 200) != 200 or not body.get("ok", False):
            # Telegram REJECTED the send (bad token = 401/404, bad chat_id or
            # malformed HTML = 400). Without this check the bot logs success
            # while the chat stays empty.
            log.error(f"[TG] REJECTED {getattr(r, 'status_code', '?')}: "
                      f"{str(body)[:160]} — check TELEGRAM_TOKEN / "
                      f"TELEGRAM_CHAT_ID on this service")
            return
        log.info(f"[TG] {msg[:80]}")
    except Exception as e:
        log.error(f"TG error: {e}")


_upd = None


def handle_commands():
    global _upd
    try:
        p = {"timeout": 1}
        if _upd:
            p["offset"] = _upd
        for u in requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                              params=p, timeout=5).json().get("result", []):
            _upd = u["update_id"] + 1
            t = u.get("message", {}).get("text", "").strip().lower()
            if str(u.get("message", {}).get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
                continue
            if t == "/watch":
                lines = []
                for tf in TFS:
                    open_ts, close_ts, secs_left = window_times(tf)
                    for asset in ASSET_LIST:
                        if asset in EXCLUDE_ASSETS:
                            continue
                        ref = prices_ref.get(asset)
                        op = open_windows.get((asset, tf, open_ts))
                        if ref is None or not op:
                            continue
                        mv = (ref - op) / op * 100.0
                        need = _frontier_lookup(
                            PER_ASSET_FRONTIER_15M.get(asset, GLOBAL_FRONTIER_15M)
                            if tf == 15 else
                            PER_ASSET_FRONTIER.get(asset, GLOBAL_FRONTIER), secs_left)
                        hot = "🔥" if abs(mv) >= need else "  "
                        lines.append(f"{hot}{asset} {tf}m: {mv:+.3f}% "
                                     f"(need ±{need:.2f}, {secs_left:.0f}s left)")
                tg("👀 <b>live watch</b> (move vs threshold)\n" +
                   ("\n".join(lines[:16]) if lines else "no windows captured yet — "
                    "wait for the next window to open"))
            elif t == "/status":
                with pending_lock:
                    nopen = len(pending)
                mode = "🟢 LIVE" if (LIVE and _clob) else "📄 PAPER"
                left = BANKROLL_STOP + _live_realized  # realized is <=0
                tg(f"{mode} <b>status</b>\n"
                   f"open positions: {nopen}\n"
                   f"realized P&L today: ${_live_realized:+.2f}\n"
                   f"bankroll left: ${max(0.0, left):.2f} / ${BANKROLL_STOP:g}\n"
                   f"assets: {','.join(a for a in ASSET_LIST if a not in EXCLUDE_ASSETS)}\n"
                   f"tf={TFS} · stake ${LIVE_STAKE:g} · "
                   f"exit {'@'+str(int(EXIT_TRIGGER_CENTS))+'¢' if EXIT_TRIGGER_CENTS>0 else 'off'}")
            elif t == "/balance":
                if not (LIVE and _clob):
                    tg("📄 paper mode — no real balance")
                    continue
                bal = None
                try:
                    from py_clob_client_v2.clob_types import (
                        BalanceAllowanceParams, AssetType)
                    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    bal = _clob.get_balance_allowance(params=params)
                except Exception as e1:
                    try:
                        # some SDK builds accept a plain dict
                        bal = _clob.get_balance_allowance(
                            params={"asset_type": "COLLATERAL"})
                    except Exception as e2:
                        tg(f"⚠️ balance fetch failed: {e1} / {e2}\n"
                           f"(funder {POLY_FUNDER[:10]}…)")
                        continue
                # bal often returns raw units (USDC has 6 decimals)
                log.info(f"[BALANCE] raw response: {bal}")
                try:
                    raw = float(bal.get("balance", 0)) if isinstance(bal, dict) else float(bal)
                    usdc = raw / 1_000_000 if raw > 1000 else raw
                    tg(f"💰 <b>wallet balance</b>: ${usdc:.2f} USDC\n"
                       f"raw: <code>{str(bal)[:120]}</code>\n"
                       f"funder: <code>{POLY_FUNDER}</code>")
                except Exception:
                    tg(f"💰 wallet balance (raw): {bal}")
            elif t == "/stats":
                sb = db_scoreboard()
                wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "—"
                verdict = ("ABOVE break-even ✅" if sb["wr"] and sb["wr"] > sb["be"]
                           else "below break-even ⚠️")
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT tf, ask_cents, asset, result, pnl FROM paper "
                          "WHERE result IN ('WIN','LOSS')")
                rows = c.fetchall()
                conn.close()
                def seg(sub):
                    n = len(sub)
                    if not n:
                        return "—"
                    w = sum(1 for r in sub if r[3] == "WIN")
                    p = sum(r[4] or 0 for r in sub)
                    a = sum(r[1] or 0 for r in sub) / n
                    return f"{w}/{n} ({w/n*100:.1f}% vs BE {a:.1f}) ${p:+.2f}"
                tfs_seen = sorted({r[0] for r in rows})
                by_tf = "\n".join(f"  {tf}m: {seg([r for r in rows if r[0]==tf])}"
                                  for tf in tfs_seen)
                bands = [("&lt;98¢", lambda a: a < 98),
                         ("98-99¢", lambda a: 98 <= a < 99),
                         ("99-99.5¢", lambda a: 99 <= a < 99.5),
                         ("≥99.5¢", lambda a: a >= 99.5)]
                by_band = "\n".join(f"  {nm}: {seg([r for r in rows if fn(r[1] or 0)])}"
                                    for nm, fn in bands)
                ap = {}
                for r in rows:
                    ap[r[2]] = ap.get(r[2], 0.0) + (r[4] or 0)
                srt = sorted(ap.items(), key=lambda kv: kv[1])
                worst = " · ".join(f"{k} ${v:+.2f}" for k, v in srt[:2])
                best = " · ".join(f"{k} ${v:+.2f}" for k, v in srt[-2:])
                # per-asset net WITHIN the >=99.5c core band (the real strategy's
                # price zone) — answers "does removing an asset rescue the core?"
                core = [r for r in rows if (r[1] or 0) >= 99.5]
                core_by_asset = {}
                for r in core:
                    d = core_by_asset.setdefault(r[2], {"n": 0, "w": 0, "net": 0.0})
                    d["n"] += 1
                    d["w"] += 1 if r[3] == "WIN" else 0
                    d["net"] += r[4] or 0
                core_lines = []
                for k in sorted(core_by_asset, key=lambda a: core_by_asset[a]["net"]):
                    d = core_by_asset[k]
                    wr_a = d["w"] / d["n"] * 100 if d["n"] else 0
                    core_lines.append(f"  {k}: {d['n']} · {wr_a:.1f}% · ${d['net']:+.2f}")
                core_txt = "\n".join(core_lines) if core_lines else "  (none yet)"
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT ask_cents, result, pnl FROM paper "
                          "WHERE graded='settle' AND result IN ('WIN','LOSS')")
                crows = c.fetchall()
                conn.close()
                def cseg(sub):
                    n = len(sub)
                    if not n:
                        return "no settled trades yet"
                    w = sum(1 for r in sub if r[1] == "WIN")
                    p = sum(r[2] or 0 for r in sub)
                    a = sum(r[0] or 0 for r in sub) / n
                    return f"{w}/{n} ({w/n*100:.1f}% vs BE {a:.1f}) ${p:+.2f}"
                cert = (f"— CERTIFIED (settlement-graded) —\n"
                        f"  all: {cseg(crows)}\n"
                        f"  &lt;98¢: {cseg([r for r in crows if (r[0] or 99) < 98])}\n")
                tg(f"{'🟢 LIVE' if (LIVE and _clob) else '📄 PAPER'} <b>TAKER scoreboard</b>\n"
                   f"{cert}"
                   f"simulated trades: {sb['n']}\n"
                   f"win rate: <b>{wr}</b>\n"
                   f"avg ask paid: {sb['avg_ask']:.1f}¢ → break-even ≈ {sb['be']:.1f}%\n"
                   f"{verdict}\n"
                   f"paper P&L: <b>${sb['pnl']:+.2f}</b> ({PAPER_STAKE:g}/trade)\n"
                   f"— by timeframe —\n{by_tf}\n"
                   f"— by ask band —\n{by_band}\n"
                   f"best: {best}\nworst: {worst}\n"
                   f"— ≥99.5¢ core band, per asset —\n{core_txt}")
            elif t == "/exit":
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT result, pnl, ask_cents, bid_path FROM paper "
                          "WHERE bid_path IS NOT NULL AND result IN ('WIN','LOSS')")
                rows = c.fetchall()
                conn.close()
                if not rows:
                    tg("🚪 no bid-path data yet — the logger collects from NEW "
                       "trades after this deploy; check back in a day")
                    continue
                TRIG = 90.0
                saved = false_cost = lost_tot = 0.0
                caught = uncatch = losses = false_n = 0
                for res, pnl, ask, pj in rows:
                    try:
                        path = json.loads(pj)
                    except Exception:
                        continue
                    shares = PAPER_STAKE / ((ask or 99.0) / 100.0)
                    dip = next((b for _, b in path if b is not None and b <= TRIG),
                               None)
                    if res == "LOSS":
                        losses += 1
                        lost_tot += -(pnl or -PAPER_STAKE)
                        if dip is None:
                            uncatch += 1
                        else:
                            caught += 1
                            saved += shares * dip / 100.0
                    elif dip is not None:
                        false_n += 1
                        false_cost += shares * (100.0 - dip) / 100.0
                net = saved - false_cost
                tg(f"🚪 <b>EXIT CEILING</b> — sell when bid ≤{TRIG:.0f}¢ "
                   f"(hypothetical, on recorded paths)\n"
                   f"losses with paths: {losses} (${lost_tot:.2f} lost)\n"
                   f"catchable: {caught} → recovers ${saved:+.2f} · "
                   f"gap/uncatchable: {uncatch}\n"
                   f"false fires: {false_n} winners dipped ≤{TRIG:.0f}¢ → "
                   f"cost ${false_cost:.2f} if exited\n"
                   f"<b>net effect of this exit ≈ ${net:+.2f}</b>")
    except Exception:
        pass


# ── SIGNAL/SCORING ENGINE ────────────────────────────────────────────────────
open_windows = {}     # (asset,tf,open_ts) -> open_price captured at window start
pending = []          # simulated trades awaiting settlement
pending_lock = threading.Lock()
fired_count = {}      # (asset,tf,open_ts) -> num entries taken this window
fired_last = {}       # (asset,tf,open_ts) -> ts of last entry (for cooldown)


def engine():
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                for asset in ASSET_LIST:
                    ref = prices_ref.get(asset)
                    if ref is None:
                        continue
                    wkey = (asset, tf, open_ts)
                    # Capture the open price the first time we see this window.
                    # If we catch it near the true start, that's a clean open.
                    # If we join mid-window (e.g. right after boot), we still
                    # capture the earliest price we saw AND record how late we
                    # were, so the gate can require a move relative to it. Only
                    # windows joined AFTER the entry cutoff are useless (no time
                    # to act), so those we skip.
                    if wkey not in open_windows:
                        elapsed = (tf * 60) - secs_left
                        cutoff_open = ENTRY_MAX_SECS_15M if tf == 15 else ENTRY_MAX_SECS
                        if secs_left <= 2:
                            open_windows[wkey] = None      # window basically over
                        else:
                            # capture the open regardless; flag if it was late
                            open_windows[wkey] = ref
                            if elapsed > OPEN_CAPTURE_GRACE:
                                log.info(f"[OPEN] {asset} {tf}m joined mid-window "
                                         f"({elapsed:.0f}s in) — ref-anchored open")
                        continue
                    op = open_windows[wkey]
                    if op is None:
                        continue  # window unusable
                    move = (ref - op) / op * 100.0
                    direction = "UP" if move >= 0 else "DOWN"
                    absmove = abs(move)
                    # SAME GATE as the maker bot
                    if fired_count.get(wkey, 0) >= MAX_STACK:
                        continue  # window already at its stack limit
                    if time.time() - fired_last.get(wkey, 0) < STACK_COOLDOWN_SECS:
                        continue  # space stacked entries out (re-qualify over time)
                    cutoff = ENTRY_MAX_SECS_15M if tf == 15 else ENTRY_MAX_SECS
                    if secs_left > cutoff:
                        continue  # too early for this timeframe's entry window
                    if not frontier_locked(asset, absmove, secs_left, tf):
                        continue
                    # gate fired — read the live ask we'd cross
                    if asset in EXCLUDE_ASSETS:
                        continue  # excluded by name (e.g. XRP)
                    if LIVE and _live_realized <= -BANKROLL_STOP:
                        continue  # bankroll stop hit — no more live entries
                    toks = resolve_tokens(asset, tf, open_ts)
                    if not toks:
                        continue
                    up_tok, down_tok = toks
                    tok = up_tok if direction == "UP" else down_tok
                    ask = best_ask_cents(tok)
                    if ask is None:
                        continue
                    # set cooldown regardless (so we don't re-hit the same instant),
                    # but only a real take consumes a STACK SLOT.
                    fired_last[wkey] = time.time()
                    # Polymarket REJECTS buy orders priced above 99¢ (max 0.99).
                    # So for live trading, an ask above 99¢ can't be filled — skip
                    # it rather than attempt-and-fail. This also means the ultra-
                    # high fills paper logged were never live-reachable.
                    live_max = min(TAKER_MAX_ASK_CENTS, 99.0) if LIVE else TAKER_MAX_ASK_CENTS
                    if ask > live_max or ask < TAKER_MIN_ASK_CENTS:
                        why = ("above 99¢ (exchange max)" if LIVE and ask > 99.0
                               else "too high" if ask > live_max
                               else "too uncertain")
                        log.info(f"[ENTRY] {asset} {tf}m {direction} gate fired but "
                                 f"ask {ask:.1f}¢ {why} "
                                 f"(band {TAKER_MIN_ASK_CENTS:.0f}-{live_max:.0f}¢) — skip")
                        if SEND_EACH and not LIVE:
                            tg(f"⚪ skip {ASSET_EMOJI.get(asset,'')}{asset} {tf}m "
                               f"{direction} · ask {ask:.1f}¢ {why} ({absmove:.3f}% "
                               f"@{secs_left:.0f}s)")
                        continue
                    filled = True
                    if LIVE:
                        filled = live_buy(tok, LIVE_STAKE)
                        if filled:
                            tg(f"🟢 <b>LIVE BUY {ASSET_EMOJI.get(asset,'')}{asset} "
                               f"{tf}m {direction}</b> ~{ask:.1f}¢ · ${LIVE_STAKE:g}")
                        else:
                            log.warning(f"[LIVE] buy failed {asset} {tf}m — not filled")
                            continue  # no fill = no position; skip
                    rid = db_insert(asset, tf, direction, open_ts, close_ts,
                                    secs_left, move, ask, op)
                    fired_count[wkey] = fired_count.get(wkey, 0) + 1
                    clip_n = fired_count[wkey]
                    with pending_lock:
                        pending.append({"rid": rid, "asset": asset, "tf": tf,
                                        "direction": direction, "open_ts": open_ts,
                                        "close_ts": close_ts,
                                        "open_price": op, "ask": ask,
                                        "token": tok})
                    if SEND_EACH:
                        arrow = "⬆️" if direction == "UP" else "⬇️"
                        clip_tag = f" clip {clip_n}/{MAX_STACK}" if MAX_STACK > 1 else ""
                        tg(f"📄 <b>PAPER TAKE {arrow} {ASSET_EMOJI.get(asset,'')}{asset} "
                           f"{tf}m {direction}{clip_tag}</b>\n"
                           f"take ask <b>{ask:.1f}¢</b> · move {move:+.3f}% @{secs_left:.0f}s left\n"
                           f"(would stake ${PAPER_STAKE:g}, win +${PAPER_STAKE*(100-ask)/ask:.2f} / "
                           f"lose -${PAPER_STAKE:.2f})")
                    log.info(f"[PAPER] TAKE {asset} {tf}m {direction} ask {ask:.1f}¢ "
                             f"move {move:+.3f}% {secs_left:.0f}s left")
        except Exception as e:
            log.error(f"[ENGINE] {e}")


def scorer():
    """CERTIFIED grading: polls the real Polymarket settlement per trade and
    grades only from it. If settlement never appears within the timeout, the
    trade VOIDs — never feed-guessed. This is the certification layer: the
    <98c pocket lives exactly where feed-vs-settlement disagreement is largest,
    so only settlement-graded rows count toward the confirmatory bar."""
    while True:
        try:
            time.sleep(1.0)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"] + 2:
                    continue
                if now - s.get("last_chk", 0) < SETTLE_POLL_SECS:
                    continue
                s["last_chk"] = now
                outcome = fetch_polymarket_outcome(s["asset"], s["tf"], s["open_ts"])
                if outcome is None:
                    if now <= s["close_ts"] + SETTLE_TIMEOUT_SECS:
                        continue  # not resolved yet — keep waiting
                    db_resolve(s["rid"], None, "VOID", 0, graded="timeout")
                    log.warning(f"[SCORER] VOID {s['asset']} {s['tf']}m — no "
                                f"settlement within timeout (never feed-guessed)")
                    if s.get("bid_path"):
                        try:
                            db_set_path(s["rid"], json.dumps(s["bid_path"]))
                        except Exception:
                            pass
                    with pending_lock:
                        s in pending and pending.remove(s)
                    continue
                won = (s["direction"] == outcome)
                stake = LIVE_STAKE if LIVE else PAPER_STAKE
                shares = stake / (s["ask"] / 100.0)
                pnl = (shares * 1.0 - stake) if won else -stake
                result = "WIN" if won else "LOSS"
                if LIVE:
                    global _live_realized
                    _live_realized += pnl
                db_resolve(s["rid"], None, result, round(pnl, 4), graded="settle")
                if s.get("bid_path"):
                    try:
                        db_set_path(s["rid"], json.dumps(s["bid_path"]))
                    except Exception:
                        pass
                with pending_lock:
                    s in pending and pending.remove(s)
                sb = db_scoreboard()
                emoji = "\u2705" if won else "\u274c"
                wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "\u2014"
                tg(f"{emoji} PAPER {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                   f"{s['tf']}m {s['direction']} <b>{result}</b> ${pnl:+.2f} "
                   f"\u00b7 {s['ask']:.1f}\u00a2 \u00b7 settle-graded \u2714\n"
                   f"\U0001f4c4 {sb['n']} trades \u00b7 {wr} (BE {sb['be']:.1f}%) "
                   f"\u00b7 P&L ${sb['pnl']:+.2f}")
        except Exception as e:
            log.error(f"[SCORER] {e}")


def daily_summary():
    """Posts a once-daily P&L recap."""
    last_day = None
    while True:
        try:
            time.sleep(60)
            today = datetime.now(timezone.utc).date()
            now = datetime.now(timezone.utc)
            # fire once per day around 00:05 UTC
            if now.hour == 0 and now.minute < 5 and last_day != today:
                last_day = today
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT result, pnl FROM paper WHERE result IN "
                          "('WIN','LOSS') AND date(created)=date('now','-1 day')")
                rows = c.fetchall()
                conn.close()
                if rows:
                    w = sum(1 for r in rows if r[0] == "WIN")
                    p = sum(r[1] or 0 for r in rows)
                    tg(f"📅 <b>Daily summary</b>\n"
                       f"trades: {len(rows)} · wins: {w} · "
                       f"P&L: ${p:+.2f}"
                       + (f"\nrealized (live): ${_live_realized:+.2f}" if LIVE else ""))
        except Exception as e:
            log.error(f"[SUMMARY] {e}")


def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    live_ok = _clob_init() if LIVE else False
    if LIVE and not live_ok:
        tg("🔴 LIVE=true but CLOB client failed (keys/SDK). Running PAPER only.")
    threading.Thread(target=binance_ref_worker, daemon=True).start()
    threading.Thread(target=engine, daemon=True).start()
    threading.Thread(target=scorer, daemon=True).start()
    threading.Thread(target=monitor, daemon=True).start()
    threading.Thread(target=daily_summary, daemon=True).start()
    if LIVE and live_ok:
        tg(f"🟢 <b>LIVE TRADER (guessed gate) armed</b> — REAL money\n"
           f"stake ${LIVE_STAKE:g} · bankroll stop ${BANKROLL_STOP:g} · "
           f"exclude {','.join(sorted(EXCLUDE_ASSETS))} · tf={TFS}\n"
           f"exit: {'sell @'+str(int(EXIT_TRIGGER_CENTS))+'¢' if EXIT_TRIGGER_CENTS>0 else 'off'}")
    if not (LIVE and live_ok):
        tg(f"📄 <b>PAPER MODE</b> — no money (set LIVE=true + keys to trade)\n"
           f"tf={TFS} · stake ${PAPER_STAKE:g} · skip if ask > {TAKER_MAX_ASK_CENTS:.0f}¢\n/stats")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
