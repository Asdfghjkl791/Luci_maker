#!/usr/bin/env python3
# PAPER PAIR-ARB v1.0 — the sum-under-a-dollar census (no money, no prediction)
#
# THE TRADE
#   UP + DOWN on the same window always pays exactly $1 per pair at settlement
#   (and a matched pair can be merged to $1 USDC instantly). So:
#     • asks summing UNDER $1  -> buy both sides  -> locked profit at entry
#     • bids summing OVER  $1  -> split & sell both -> locked profit at entry
#   No direction, no move threshold, no entry window, no win rate, no grading.
#   The ONLY trigger is the two books disagreeing with each other.
#
# WHAT THIS BOT DOES
#   Scans every asset/timeframe window, records every "sighting" where the
#   locked edge exceeds PAIR_MIN_EDGE_CENTS with at least PAIR_MIN_DEPTH shares
#   on both sides. Tracks how LONG each sighting survives (the capturability
#   question) and tallies the pennies that were there. Pure census, no orders.
#
# HONEST LIMITS
#   • Sightings at our polling speed are what WE could see — fast bots snipe
#     crossed books in milliseconds, so real capture is a race we'd often lose.
#   • Paper assumes both legs fill at displayed size (real leg-risk not modeled).
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): DB_PATH, TIMEFRAMES=5, PAIR_POLL_SECS=3,
#   PAIR_MIN_EDGE_CENTS=0.5, PAIR_MIN_DEPTH=5, PAIR_SHARES=5, SEND_EACH=true

import os
import time
import json
import sqlite3
import logging
import threading
import requests
from datetime import datetime, timezone

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
DB_PATH          = os.environ.get("DB_PATH", "paper_pairarb.db")
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5").split(",")]
PAIR_POLL_SECS       = float(os.environ.get("PAIR_POLL_SECS", "3"))
PAIR_MIN_EDGE_CENTS  = float(os.environ.get("PAIR_MIN_EDGE_CENTS", "0.5"))
PAIR_MIN_DEPTH       = float(os.environ.get("PAIR_MIN_DEPTH", "5"))
PAIR_SHARES          = float(os.environ.get("PAIR_SHARES", "5"))
SEND_EACH        = os.environ.get("SEND_EACH", "true").lower() == "true"
HEARTBEAT_SECS   = int(os.environ.get("HEARTBEAT_SECS", "60"))

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("paper-pairarb")

# ─── market resolution (same as the other paper bots) ────────────────────────
_market_cache = {}

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
                _market_cache[key] = (toks[0], toks[1])
                return _market_cache[key]
    except Exception as e:
        log.debug(f"[RESOLVE] {slug}: {e}")
    _market_cache[key] = None
    return None


def fetch_book_top(token_id):
    """Best ask/bid with size, in cents: (ask_c, ask_sz, bid_c, bid_sz) or None."""
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        asks = [(float(a["price"]), float(a.get("size", 0)))
                for a in b.get("asks", []) if float(a.get("size", 0)) > 0]
        bids = [(float(x["price"]), float(x.get("size", 0)))
                for x in b.get("bids", []) if float(x.get("size", 0)) > 0]
        if not asks or not bids:
            return None
        ap = min(p for p, _ in asks)
        bp = max(p for p, _ in bids)
        asz = sum(s for p, s in asks if p == ap)
        bsz = sum(s for p, s in bids if p == bp)
        return (ap * 100.0, asz, bp * 100.0, bsz)
    except Exception:
        return None


def window_times(tf):
    now = int(time.time())
    length = tf * 60
    open_ts = (now // length) * length
    return open_ts, open_ts + length, open_ts + length - now


# ─── DB ──────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS sightings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT,
        asset TEXT, tf INTEGER, open_ts INTEGER, secs_left REAL,
        side TEXT, sum_cents REAL, edge_cents REAL,
        pairs REAL, locked_usd REAL, duration_secs REAL)""")
    conn.commit()
    conn.close()


def db_insert_sighting(asset, tf, open_ts, secs_left, side, sum_c, edge, pairs, locked):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO sightings (created,asset,tf,open_ts,secs_left,side,
                 sum_cents,edge_cents,pairs,locked_usd,duration_secs)
                 VALUES (?,?,?,?,?,?,?,?,?,?,NULL)""",
              (datetime.now(timezone.utc).isoformat(), asset, tf, open_ts,
               secs_left, side, sum_c, edge, pairs, locked))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_update_sighting(rid, edge=None, locked=None, duration=None):
    conn = sqlite3.connect(DB_PATH)
    if edge is not None:
        conn.execute("UPDATE sightings SET edge_cents=?, locked_usd=? WHERE id=?",
                     (edge, locked, rid))
    if duration is not None:
        conn.execute("UPDATE sightings SET duration_secs=? WHERE id=?", (duration, rid))
    conn.commit()
    conn.close()


# ─── TELEGRAM ────────────────────────────────────────────────────────────────
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
            log.error(f"[TG] REJECTED {getattr(r, 'status_code', '?')}: "
                      f"{str(body)[:160]} — check TELEGRAM_TOKEN / TELEGRAM_CHAT_ID")
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
            if t == "/stats":
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT side, edge_cents, locked_usd, duration_secs FROM sightings")
                rows = c.fetchall()
                conn.close()
                if not rows:
                    tg("💵 <b>PAIR-ARB census</b>\nno sightings yet — the books have "
                       "summed to ≥100¢ every scan (tight market = correct silence)")
                    continue
                def sec(side):
                    sub = [r for r in rows if r[0] == side]
                    if not sub:
                        return f"{side}: none"
                    n = len(sub)
                    avg_e = sum(r[1] for r in sub) / n
                    mx_e = max(r[1] for r in sub)
                    tot = sum(r[2] or 0 for r in sub)
                    durs = [r[3] for r in sub if r[3] is not None]
                    avg_d = (sum(durs) / len(durs)) if durs else 0
                    return (f"{side}: {n} sightings · edge avg {avg_e:.1f}¢ max {mx_e:.1f}¢ "
                            f"· lived ~{avg_d:.0f}s · locked ${tot:+.2f}")
                tg("💵 <b>PAIR-ARB census</b>\n" + sec("BUY") + "\n" + sec("SELL") +
                   f"\n(edge ≥{PAIR_MIN_EDGE_CENTS:.1f}¢, depth ≥{PAIR_MIN_DEPTH:.0f} "
                   f"each side, {PAIR_SHARES:.0f} pairs/sighting)")
    except Exception:
        pass


# ─── SCANNER ─────────────────────────────────────────────────────────────────
active = {}   # (asset, tf, open_ts, side) -> {rid, entered, best_edge}

def scanner():
    while True:
        try:
            time.sleep(PAIR_POLL_SECS)
            now = time.time()
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                if secs_left <= 2:
                    continue
                for asset in ASSET_LIST:
                    toks = resolve_tokens(asset, tf, open_ts)
                    if not toks:
                        continue
                    up = fetch_book_top(toks[0])
                    dn = fetch_book_top(toks[1])
                    if not up or not dn:
                        continue
                    ua, usz, ub, ubsz = up
                    da, dsz, db_, dbsz = dn

                    # BUY side: asks sum under par
                    sum_a = ua + da
                    edge_a = 100.0 - sum_a
                    ok_a = (edge_a >= PAIR_MIN_EDGE_CENTS and
                            min(usz, dsz) >= PAIR_MIN_DEPTH)
                    _episode(asset, tf, open_ts, secs_left, "BUY", sum_a, edge_a,
                             min(PAIR_SHARES, usz, dsz), ok_a, now)

                    # SELL side: bids sum over par (split $1 and sell both)
                    sum_b = ub + db_
                    edge_b = sum_b - 100.0
                    ok_b = (edge_b >= PAIR_MIN_EDGE_CENTS and
                            min(ubsz, dbsz) >= PAIR_MIN_DEPTH)
                    _episode(asset, tf, open_ts, secs_left, "SELL", sum_b, edge_b,
                             min(PAIR_SHARES, ubsz, dbsz), ok_b, now)

            # close episodes whose windows ended
            for key in list(active.keys()):
                a, tf, ots, side = key
                if time.time() > ots + tf * 60:
                    ep = active.pop(key)
                    db_update_sighting(ep["rid"], duration=round(time.time() - ep["entered"], 1))
        except Exception as e:
            log.error(f"[SCAN] {e}")


def _episode(asset, tf, open_ts, secs_left, side, sum_c, edge, pairs, in_cross, now):
    key = (asset, tf, open_ts, side)
    ep = active.get(key)
    if in_cross and ep is None:
        locked = round(pairs * edge / 100.0, 4)
        rid = db_insert_sighting(asset, tf, open_ts, secs_left, side,
                                 round(sum_c, 2), round(edge, 2), pairs, locked)
        active[key] = {"rid": rid, "entered": now, "best_edge": edge, "pairs": pairs}
        log.info(f"[PAIR] {side} {asset} {tf}m sum {sum_c:.1f}¢ edge {edge:.1f}¢ "
                 f"x{pairs:g} pairs = ${locked:+.2f} locked · {secs_left:.0f}s left")
        if SEND_EACH:
            verb = "buy both" if side == "BUY" else "split &amp; sell both"
            tg(f"💵 <b>PAIR ARB · {ASSET_EMOJI.get(asset,'')}{asset} {tf}m</b>\n"
               f"{verb}: sum <b>{sum_c:.1f}¢</b> → edge {edge:.1f}¢ × {pairs:g} pairs "
               f"= <b>${locked:+.2f} locked</b> · {secs_left:.0f}s left")
    elif in_cross and ep is not None:
        if edge > ep["best_edge"]:
            ep["best_edge"] = edge
            locked = round(ep["pairs"] * edge / 100.0, 4)
            db_update_sighting(ep["rid"], edge=round(edge, 2), locked=locked)
    elif not in_cross and ep is not None:
        active.pop(key, None)
        db_update_sighting(ep["rid"], duration=round(now - ep["entered"], 1))
        log.info(f"[PAIR] {side} {asset} {tf}m closed after "
                 f"{now - ep['entered']:.1f}s (best edge {ep['best_edge']:.1f}¢)")


def main():
    init_db()
    threading.Thread(target=scanner, daemon=True).start()
    tg(f"💵 <b>PAPER PAIR-ARB live</b> — no money, no prediction\n"
       f"scanning for UP+DOWN asks summing under 100¢ (and bids over) — "
       f"profit locked by arithmetic, direction irrelevant\n"
       f"edge ≥{PAIR_MIN_EDGE_CENTS:.1f}¢ · depth ≥{PAIR_MIN_DEPTH:.0f}/side · "
       f"tf={TFS} · poll {PAIR_POLL_SECS:.0f}s\n"
       f"silence = books are tight = correct\n/stats")
    hb = 0
    while True:
        try:
            handle_commands()
            if HEARTBEAT_SECS and time.time() - hb >= HEARTBEAT_SECS:
                hb = time.time()
                log.info(f"[Heartbeat] open episodes={len(active)}")
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
