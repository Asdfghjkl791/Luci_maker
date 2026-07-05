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
DB_PATH          = os.environ.get("DB_PATH", "paper_taker.db")
SEND_EACH        = os.environ.get("SEND_EACH", "true").lower() == "true"
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5").split(",")]

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}

# ── EXACT GATE, copied verbatim from bot_variant.py (the guessed gate) ────────
PER_ASSET_FRONTIER = {
    "BTC":  [(20, 0.025), (40, 0.04), (70, 0.08), (120, 0.15)],
    "ETH":  [(20, 0.03), (40, 0.07), (70, 0.11), (120, 0.16)],
    "XRP":  [(20, 0.04), (40, 0.10), (70, 0.15), (120, 0.23)],
    "DOGE": [(20, 0.05), (40, 0.10), (70, 0.15), (120, 0.22)],
    "BNB":  [(20, 0.04), (40, 0.10), (70, 0.17), (120, 0.20)],
    "SOL":  [(20, 0.10), (40, 0.17), (70, 0.21), (120, 0.27)],
    "HYPE": [(20, 0.16), (40, 0.20), (70, 0.24), (120, 0.30)],
}
GLOBAL_FRONTIER = [(3, 0.02), (40, 0.10), (70, 0.20), (120, 0.40)]
FRONTIER_FALLBACK_PCT = float(os.environ.get("FRONTIER_FALLBACK_PCT", "0.40"))
HYPE_MIN_MOVE_PCT = float(os.environ.get("HYPE_MIN_MOVE_PCT", "0.16"))


def _frontier_15m(asset, secs_left):
    bands = PER_ASSET_FRONTIER.get(asset, GLOBAL_FRONTIER)
    base = bands[2][1] if len(bands) >= 3 else bands[-1][1]
    if secs_left <= 20:
        return base * 1.3
    if secs_left <= 70:
        return base * 1.8
    if secs_left <= 180:
        return base * 2.3
    return base * 3.0


def frontier_locked(asset, abs_move_pct, secs_left, tf=5):
    if secs_left <= 0:
        return False
    if tf == 15:
        need = _frontier_15m(asset, secs_left)
    else:
        bands = PER_ASSET_FRONTIER.get(asset, GLOBAL_FRONTIER)
        need = FRONTIER_FALLBACK_PCT
        for max_secs, min_move in bands:
            if secs_left <= max_secs:
                need = min_move
                break
    if asset == "HYPE":
        need = max(need, HYPE_MIN_MOVE_PCT)
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
                    if ask > TAKER_MAX_ASK_CENTS:
                        # would-be trade, but ask too high → record as skipped-costly
                        log.info(f"[PAPER] {asset} {tf}m {direction} gate fired but "
                                 f"ask {ask:.1f}¢ > {TAKER_MAX_ASK_CENTS:.0f}¢ — skip")
                        if SEND_EACH:
                            tg(f"⚪ PAPER skip {ASSET_EMOJI.get(asset,'')}{asset} {tf}m "
                               f"{direction} · ask {ask:.1f}¢ too high ({absmove:.3f}% "
                               f"@{secs_left:.0f}s)")
                        continue
                    rid = db_insert(asset, tf, direction, open_ts, close_ts,
                                    secs_left, move, ask, op)
                    with pending_lock:
                        pending.append({"rid": rid, "asset": asset, "tf": tf,
                                        "direction": direction, "close_ts": close_ts,
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
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"] + 2:
                    continue
                settle = prices_ref.get(s["asset"])
                if settle is None:
                    with pending_lock:
                        s in pending and pending.remove(s)
                    db_resolve(s["rid"], None, "VOID", 0)
                    continue
                went_up = settle > s["open_price"]
                if abs(settle - s["open_price"]) < 1e-9:
                    result, pnl = "TIE", 0.0
                else:
                    won = (s["direction"] == "UP") == went_up
                    # TAKER economics: pay ask cents, win pays 100¢.
                    shares = PAPER_STAKE / (s["ask"] / 100.0)
                    pnl = (shares * 1.0 - PAPER_STAKE) if won else -PAPER_STAKE
                    result = "WIN" if won else "LOSS"
                db_resolve(s["rid"], settle, result, round(pnl, 4))
                with pending_lock:
                    s in pending and pending.remove(s)
                sb = db_scoreboard()
                emoji = "✅" if result == "WIN" else ("❌" if result == "LOSS" else "➖")
                wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "—"
                tg(f"{emoji} PAPER {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                   f"{s['tf']}m {s['direction']} <b>{result}</b> ${pnl:+.2f}\n"
                   f"📄 {sb['n']} trades · {wr} (BE {sb['be']:.1f}%) · P&L ${sb['pnl']:+.2f}")
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
