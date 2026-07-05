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
# Fast grading: grade instantly off the reference feed at window close (~1s),
# then confirm against real Polymarket settlement when it lands and correct any
# disagreement. FAST_GRADE=true gives ~1s results; false uses settlement-only
# (accurate but 1-3 min). Epsilon: a window only provisionally ties if |move| is
# below this (in %), else it grades UP/DOWN immediately.
FAST_GRADE       = os.environ.get("FAST_GRADE", "true").lower() == "true"
FAST_TIE_EPS_PCT = float(os.environ.get("FAST_TIE_EPS_PCT", "0.0005"))
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5").split(",")]

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}

# ── GATE: guessed 5m table (from bot_variant.py) + measured 15m table (from
#    bot_variant_MEASURED.py). This is the config you asked to paper-test:
#    your real guessed 5-minute strategy, paired with the probe-MEASURED 15m
#    surface (more defensible than the guessed scaling rule for 15m). ──
PER_ASSET_FRONTIER = {          # 5m — GUESSED (matches live bot_variant.py)
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
HYPE_MIN_MOVE_PCT = float(os.environ.get("HYPE_MIN_MOVE_PCT", "0.16"))


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
log = logging.getLogger("paper-taker")

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
        if abs(up_p - 0.5) < 0.01 and abs(down_p - 0.5) < 0.01:
            log.info(f"[OUTCOME] {slug}: TIE {op}")
            return "TIE"
        log.info(f"[OUTCOME] {slug}: not settled yet (prices {op})")
        return None
    except Exception as e:
        log.warning(f"[OUTCOME] {slug}: ERROR {e}")
        return None


# ── WINDOW TIMING ────────────────────────────────────────────────────────────
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


def db_resolve(rid, settle_price, result, pnl):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE paper SET settle_price=?, result=?, pnl=? WHERE id=?",
                 (settle_price, result, pnl, rid))
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
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML"}, timeout=8)
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
            if t == "/stats":
                s = db_scoreboard()
                wr = f"{s['wr']:.1f}%" if s["wr"] is not None else "—"
                verdict = ("ABOVE break-even ✅" if s["wr"] and s["wr"] > s["be"]
                           else "below break-even ⚠️")
                tg(f"📄 <b>PAPER TAKER scoreboard</b>\n"
                   f"simulated trades: {s['n']}\n"
                   f"win rate: <b>{wr}</b>\n"
                   f"avg ask paid: {s['avg_ask']:.1f}¢ → break-even ≈ {s['be']:.1f}%\n"
                   f"{verdict}\n"
                   f"paper P&L: <b>${s['pnl']:+.2f}</b> ({PAPER_STAKE:g}/trade)")
    except Exception:
        pass


# ── SIGNAL/SCORING ENGINE ────────────────────────────────────────────────────
open_windows = {}     # (asset,tf,open_ts) -> open_price captured at window start
pending = []          # simulated trades awaiting settlement
pending_lock = threading.Lock()
fired = set()         # (asset,tf,open_ts) already taken (one sim entry per window)


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
                    # capture open price once
                    if wkey not in open_windows:
                        open_windows[wkey] = ref
                        continue
                    op = open_windows[wkey]
                    move = (ref - op) / op * 100.0
                    direction = "UP" if move >= 0 else "DOWN"
                    absmove = abs(move)
                    # SAME GATE as the maker bot
                    if wkey in fired:
                        continue
                    if not frontier_locked(asset, absmove, secs_left, tf):
                        continue
                    # gate fired — simulate TAKING: read the live ask we'd cross
                    toks = resolve_tokens(asset, tf, open_ts)
                    if not toks:
                        continue
                    up_tok, down_tok = toks
                    tok = up_tok if direction == "UP" else down_tok
                    ask = best_ask_cents(tok)
                    if ask is None:
                        continue
                    fired.add(wkey)
                    if ask > TAKER_MAX_ASK_CENTS or ask < TAKER_MIN_ASK_CENTS:
                        # outside the accepted band — record why and skip
                        why = ("too high" if ask > TAKER_MAX_ASK_CENTS
                               else "too uncertain")
                        log.info(f"[PAPER] {asset} {tf}m {direction} gate fired but "
                                 f"ask {ask:.1f}¢ {why} "
                                 f"(band {TAKER_MIN_ASK_CENTS:.0f}-{TAKER_MAX_ASK_CENTS:.0f}¢) — skip")
                        if SEND_EACH:
                            tg(f"⚪ PAPER skip {ASSET_EMOJI.get(asset,'')}{asset} {tf}m "
                               f"{direction} · ask {ask:.1f}¢ {why} ({absmove:.3f}% "
                               f"@{secs_left:.0f}s)")
                        continue
                    rid = db_insert(asset, tf, direction, open_ts, close_ts,
                                    secs_left, move, ask, op)
                    with pending_lock:
                        pending.append({"rid": rid, "asset": asset, "tf": tf,
                                        "direction": direction, "open_ts": open_ts,
                                        "close_ts": close_ts,
                                        "open_price": op, "ask": ask})
                    if SEND_EACH:
                        arrow = "⬆️" if direction == "UP" else "⬇️"
                        tg(f"📄 <b>PAPER TAKE {arrow} {ASSET_EMOJI.get(asset,'')}{asset} "
                           f"{tf}m {direction}</b>\n"
                           f"take ask <b>{ask:.1f}¢</b> · move {move:+.3f}% @{secs_left:.0f}s left\n"
                           f"(would stake ${PAPER_STAKE:g}, win +${PAPER_STAKE*(100-ask)/ask:.2f} / "
                           f"lose -${PAPER_STAKE:.2f})")
                    log.info(f"[PAPER] TAKE {asset} {tf}m {direction} ask {ask:.1f}¢ "
                             f"move {move:+.3f}% {secs_left:.0f}s left")
        except Exception as e:
            log.error(f"[ENGINE] {e}")


def scorer():
    """Two-tier grading. TIER 1 (fast, ~1s): at window close, grade off the
    reference feed and post immediately, tagged with a lightning bolt. TIER 2
    (confirm, 1-3 min): when Polymarket publishes the real settlement, check it;
    if it disagrees, post a correction and fix the DB. ~1s feedback that
    self-corrects to ground truth."""
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"]:
                    continue

                # TIER 1: fast provisional grade off the feed (once)
                if FAST_GRADE and not s.get("fast_done"):
                    settle = prices_ref.get(s["asset"])
                    op = s["open_price"]
                    if settle is None:
                        if now > s["close_ts"] + 20:
                            s["fast_done"] = True
                        continue
                    s["fast_done"] = True
                    move = (settle - op) / op * 100.0
                    if abs(move) < FAST_TIE_EPS_PCT:
                        result, pnl = "TIE", 0.0
                    else:
                        won = (s["direction"] == ("UP" if move > 0 else "DOWN"))
                        shares = PAPER_STAKE / (s["ask"] / 100.0)
                        pnl = (shares * 1.0 - PAPER_STAKE) if won else -PAPER_STAKE
                        result = "WIN" if won else "LOSS"
                    s["fast_result"] = result
                    db_resolve(s["rid"], None, result, round(pnl, 4))
                    sb = db_scoreboard()
                    emoji = "\u2705" if result == "WIN" else ("\u274c" if result == "LOSS" else "\u2796")
                    wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "\u2014"
                    tg(f"{emoji}\u26a1 PAPER {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                       f"{s['tf']}m {s['direction']} <b>{result}</b> ${pnl:+.2f}\n"
                       f"\U0001f4c4 {sb['n']} trades \u00b7 {wr} (BE {sb['be']:.1f}%) \u00b7 P&L ${sb['pnl']:+.2f}")

                # TIER 2: confirm against real Polymarket settlement
                lastck = s.get("last_outcome_check", 0)
                if now - lastck < 15:
                    continue
                s["last_outcome_check"] = now
                outcome = fetch_polymarket_outcome(s["asset"], s["tf"], s["open_ts"])
                if outcome is None:
                    if now > s["close_ts"] + 600:
                        if not s.get("fast_done"):
                            db_resolve(s["rid"], None, "VOID", 0)
                        log.warning(f"[SCORER] no settlement after 10min {s['asset']} {s['tf']}m")
                        with pending_lock:
                            s in pending and pending.remove(s)
                    continue
                if outcome == "TIE":
                    true_result, true_pnl = "TIE", 0.0
                else:
                    won = (s["direction"] == outcome)
                    shares = PAPER_STAKE / (s["ask"] / 100.0)
                    true_pnl = (shares * 1.0 - PAPER_STAKE) if won else -PAPER_STAKE
                    true_result = "WIN" if won else "LOSS"
                if s.get("fast_done") and s.get("fast_result") != true_result:
                    db_resolve(s["rid"], None, true_result, round(true_pnl, 4))
                    tg(f"\U0001f527 CORRECTION {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                       f"{s['tf']}m {s['direction']}: fast said {s.get('fast_result')}, "
                       f"settlement says <b>{true_result}</b> \u2014 scoreboard fixed")
                    log.info(f"[SCORER] corrected {s['asset']} {s['tf']}m {s.get('fast_result')}->{true_result}")
                elif not s.get("fast_done"):
                    db_resolve(s["rid"], None, true_result, round(true_pnl, 4))
                    sb = db_scoreboard()
                    emoji = "\u2705" if true_result == "WIN" else ("\u274c" if true_result == "LOSS" else "\u2796")
                    wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "\u2014"
                    tg(f"{emoji} PAPER {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                       f"{s['tf']}m {s['direction']} <b>{true_result}</b> ${true_pnl:+.2f}\n"
                       f"\U0001f4c4 {sb['n']} trades \u00b7 {wr} (BE {sb['be']:.1f}%) \u00b7 P&L ${sb['pnl']:+.2f}")
                with pending_lock:
                    s in pending and pending.remove(s)
        except Exception as e:
            log.error(f"[SCORER] {e}")

def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    threading.Thread(target=binance_ref_worker, daemon=True).start()
    threading.Thread(target=engine, daemon=True).start()
    threading.Thread(target=scorer, daemon=True).start()
    tg(f"📄 <b>PAPER TAKER live</b> — no money\n"
       f"same gate as the maker bot; simulates TAKING the ask instead of resting a bid\n"
       f"tf={TFS} · stake ${PAPER_STAKE:g} · skip if ask > {TAKER_MAX_ASK_CENTS:.0f}¢\n"
       f"the scoreboard decides: does taking clear break-even?\n/stats")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
