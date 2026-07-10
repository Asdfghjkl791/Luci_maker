#!/usr/bin/env python3
# PAIR-ARB WS CENSUS — millisecond-latency sum-under-a-dollar census (no money)
#
# WHY THIS EXISTS
#   The 3-second REST poller both invented phantoms (sequential reads) and
#   MISSED every real cross living under ~3s. This version subscribes to
#   Polymarket's live book websocket for all 14 tokens and maintains local
#   top-of-book, so crosses are detected in ~0.1s and their LIFETIMES are
#   measured precisely. The output that decides everything: how many real
#   crosses/day exist, and the duration histogram — 2s+ crosses are humanly
#   race-able; sub-0.5s ones belong to colocated bots forever.
#
# STILL ZERO MONEY. This is the gate for any live test: if the reachable rate
# is real, the live executor conversation happens; if not, the edge is closed.
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): DB_PATH, PAIR_MIN_EDGE_CENTS=0.5, PAIR_MIN_DEPTH=5,
#   PAIR_SHARES=5, PAIR_TG_MIN_EDGE=1.0 (only telegram crosses >= this),
#   WS_DEBUG=false (log first raw ws messages to verify field names)

import os
import time
import json
import sqlite3
import logging
import threading
import requests
from datetime import datetime, timezone

import websocket

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
DB_PATH          = os.environ.get("DB_PATH", "pair_ws.db")
PAIR_MIN_EDGE_CENTS = float(os.environ.get("PAIR_MIN_EDGE_CENTS", "0.5"))
PAIR_MIN_DEPTH      = float(os.environ.get("PAIR_MIN_DEPTH", "5"))
PAIR_SHARES         = float(os.environ.get("PAIR_SHARES", "5"))
PAIR_TG_MIN_EDGE    = float(os.environ.get("PAIR_TG_MIN_EDGE", "1.0"))
WS_DEBUG        = os.environ.get("WS_DEBUG", "false").lower() == "true"
HEARTBEAT_SECS  = int(os.environ.get("HEARTBEAT_SECS", "300"))
TF = 5  # 5m windows only in this version

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
GAMMA_BASE = "https://gamma-api.polymarket.com"
WS_URL = os.environ.get("WS_URL",
                        "wss://ws-subscriptions-clob.polymarket.com/ws/market")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("pair-ws")


def window_times(tf=TF):
    now = time.time()
    length = tf * 60
    open_ts = int(now // length) * length
    return open_ts, open_ts + length, open_ts + length - now


def resolve_tokens(asset, tf, open_ts, tries=3):
    slug = f"{asset.lower()}-updown-{tf}m-{open_ts}"
    for _ in range(tries):
        try:
            r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=8)
            arr = r.json()
            ev = arr[0] if isinstance(arr, list) and arr else arr
            markets = ev.get("markets", []) if isinstance(ev, dict) else []
            if markets:
                toks = json.loads(markets[0].get("clobTokenIds", "[]"))
                if len(toks) == 2:
                    return (toks[0], toks[1])
        except Exception as e:
            log.debug(f"[RESOLVE] {slug}: {e}")
        time.sleep(1)
    return None


# ─── DB (same schema family as the poller census) ────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS sightings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT,
        asset TEXT, tf INTEGER, open_ts INTEGER, secs_left REAL,
        side TEXT, sum_cents REAL, edge_cents REAL,
        pairs REAL, locked_usd REAL, duration_secs REAL,
        depth_avail REAL, confirmed INTEGER DEFAULT 1)""")
    conn.commit()
    conn.close()


def db_insert(asset, secs_left, side, sum_c, edge, pairs, locked, depth):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO sightings (created,asset,tf,open_ts,secs_left,side,
                 sum_cents,edge_cents,pairs,locked_usd,duration_secs,depth_avail,
                 confirmed) VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?,1)""",
              (datetime.now(timezone.utc).isoformat(), asset, TF,
               int(window_times()[0]), secs_left, side, sum_c, edge, pairs,
               locked, depth))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_update(rid, edge=None, locked=None, duration=None):
    conn = sqlite3.connect(DB_PATH)
    if edge is not None:
        conn.execute("UPDATE sightings SET edge_cents=?, locked_usd=? WHERE id=?",
                     (edge, locked, rid))
    if duration is not None:
        conn.execute("UPDATE sightings SET duration_secs=? WHERE id=?",
                     (duration, rid))
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
                      f"{str(body)[:160]}")
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
                c.execute("SELECT side, edge_cents, locked_usd, duration_secs, "
                          "asset, depth_avail, secs_left FROM sightings")
                rows = c.fetchall()
                conn.close()
                if not rows:
                    tg("⚡ <b>PAIR-ARB WS census</b>\nno crosses yet — books tight "
                       "at millisecond resolution too")
                    continue
                def sec(side):
                    sub = [r for r in rows if r[0] == side]
                    if not sub:
                        return f"{side}: none"
                    n = len(sub)
                    avg_e = sum(r[1] for r in sub) / n
                    mx_e = max(r[1] for r in sub)
                    tot = sum(r[2] or 0 for r in sub)
                    return (f"{side}: {n} · edge avg {avg_e:.1f}¢ max {mx_e:.1f}¢ "
                            f"· ${tot:+.2f}")
                durs = [r[3] for r in rows if r[3] is not None]
                d_sub05 = sum(1 for d in durs if d < 0.5)
                d_05_2 = sum(1 for d in durs if 0.5 <= d < 2)
                d_2_10 = sum(1 for d in durs if 2 <= d < 10)
                d_10p = sum(1 for d in durs if d >= 10)
                assets = {}
                for r in rows:
                    assets.setdefault(r[4], [0, 0.0])
                    assets[r[4]][0] += 1
                    assets[r[4]][1] += (r[2] or 0)
                top = sorted(assets.items(), key=lambda kv: -kv[1][1])[:4]
                alines = " · ".join(f"{a} {n}x ${v:+.2f}" for a, (n, v) in top)
                reach = d_2_10 + d_10p
                tg("⚡ <b>PAIR-ARB WS census</b> (live book feed)\n"
                   + sec("BUY") + "\n" + sec("SELL") + "\n"
                   f"lifetimes: &lt;0.5s: {d_sub05} · 0.5-2s: {d_05_2} · "
                   f"2-10s: {d_2_10} · 10s+: {d_10p}\n"
                   f"<b>race-able (2s+): {reach}</b>\n"
                   f"top: {alines}")
    except Exception:
        pass


# ─── LOCAL BOOKS ─────────────────────────────────────────────────────────────
books = {}          # token -> {"bids": {price: size}, "asks": {price: size}}
token_asset = {}    # token -> (asset, "up"/"down")
pair_tokens = {}    # asset -> (up_token, down_token)
active = {}         # (asset, side) -> {rid, entered, best_edge, pairs}
stats_events = {"n": 0}


def _levels(msg, *names):
    for n in names:
        if n in msg and isinstance(msg[n], list):
            return msg[n]
    return []


def apply_book(tok, msg):
    """Full snapshot. Handles both bids/asks and buys/sells field spellings."""
    b = {}
    a = {}
    for lv in _levels(msg, "bids", "buys"):
        p, sz = float(lv.get("price", 0)), float(lv.get("size", 0))
        if p > 0 and sz > 0:
            b[p] = sz
    for lv in _levels(msg, "asks", "sells"):
        p, sz = float(lv.get("price", 0)), float(lv.get("size", 0))
        if p > 0 and sz > 0:
            a[p] = sz
    books[tok] = {"bids": b, "asks": a}


def apply_change(tok, msg):
    bk = books.get(tok)
    if bk is None:
        return
    for ch in msg.get("changes", []):
        try:
            p = float(ch.get("price", 0))
            sz = float(ch.get("size", 0))
            side = ch.get("side", "").upper()
        except Exception:
            continue
        d = bk["bids"] if side == "BUY" else bk["asks"]
        if sz <= 0:
            d.pop(p, None)
        else:
            d[p] = sz


def top_of_book(tok):
    bk = books.get(tok)
    if not bk or not bk["asks"] or not bk["bids"]:
        return None
    ap = min(bk["asks"])
    bp = max(bk["bids"])
    return (ap * 100.0, bk["asks"][ap], bp * 100.0, bk["bids"][bp])


def check_pair(asset):
    toks = pair_tokens.get(asset)
    if not toks:
        return
    up = top_of_book(toks[0])
    dn = top_of_book(toks[1])
    if not up or not dn:
        return
    now = time.time()
    _, close_ts, secs_left = window_times()
    ua, usz, ub, ubsz = up
    da, dsz, db_, dbsz = dn
    _side(asset, "BUY", ua + da, 100.0 - (ua + da), min(usz, dsz), secs_left, now)
    _side(asset, "SELL", ub + db_, (ub + db_) - 100.0, min(ubsz, dbsz), secs_left, now)


def _side(asset, side, sum_c, edge, depth, secs_left, now):
    key = (asset, side)
    ep = active.get(key)
    in_cross = (edge >= PAIR_MIN_EDGE_CENTS and depth >= PAIR_MIN_DEPTH)
    if in_cross and ep is None:
        pairs = min(PAIR_SHARES, depth)
        locked = round(pairs * edge / 100.0, 4)
        rid = db_insert(asset, round(secs_left, 1), side, round(sum_c, 2),
                        round(edge, 2), pairs, locked, depth)
        active[key] = {"rid": rid, "entered": now, "best_edge": edge,
                       "pairs": pairs}
        log.info(f"[WS-PAIR] {side} {asset} sum {sum_c:.1f}¢ edge {edge:.1f}¢ "
                 f"depth {depth:g} · {secs_left:.0f}s left")
        if edge >= PAIR_TG_MIN_EDGE:
            verb = "buy both" if side == "BUY" else "split &amp; sell both"
            tg(f"⚡ <b>WS PAIR · {ASSET_EMOJI.get(asset,'')}{asset}</b> {verb}: "
               f"sum <b>{sum_c:.1f}¢</b> edge {edge:.1f}¢ · depth {depth:g} · "
               f"{secs_left:.0f}s left")
    elif in_cross and ep is not None:
        if edge > ep["best_edge"]:
            ep["best_edge"] = edge
            locked = round(ep["pairs"] * edge / 100.0, 4)
            db_update(ep["rid"], edge=round(edge, 2), locked=locked)
    elif not in_cross and ep is not None:
        active.pop(key, None)
        dur = round(now - ep["entered"], 3)
        db_update(ep["rid"], duration=dur)
        log.info(f"[WS-PAIR] {side} {asset} closed after {dur:.2f}s "
                 f"(best {ep['best_edge']:.1f}¢)")


def close_all(now):
    for key in list(active.keys()):
        ep = active.pop(key)
        db_update(ep["rid"], duration=round(now - ep["entered"], 3))


# ─── WS LOOP: one connection per 5-minute window generation ─────────────────
def ws_loop():
    while True:
        try:
            open_ts, close_ts, secs_left = window_times()
            if secs_left < 3:
                time.sleep(secs_left + 0.5)
                continue
            # resolve this window's 14 tokens
            books.clear()
            token_asset.clear()
            pair_tokens.clear()
            all_toks = []
            for a in ASSET_LIST:
                toks = resolve_tokens(a, TF, int(open_ts))
                if not toks:
                    log.warning(f"[WS] no market for {a} this window")
                    continue
                pair_tokens[a] = toks
                token_asset[toks[0]] = (a, "up")
                token_asset[toks[1]] = (a, "down")
                all_toks += [toks[0], toks[1]]
            if not all_toks:
                time.sleep(5)
                continue
            ws = websocket.create_connection(WS_URL, timeout=10)
            ws.settimeout(5)
            ws.send(json.dumps({"type": "market", "assets_ids": all_toks,
                                "asset_ids": all_toks}))
            log.info(f"[WS] subscribed {len(all_toks)} tokens · window "
                     f"{int(secs_left)}s remaining")
            dbg = 0
            last_ping = time.time()
            while time.time() < close_ts - 0.3:
                if time.time() - last_ping > 10:
                    try:
                        ws.send("PING")
                    except Exception:
                        break
                    last_ping = time.time()
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw or raw == "PONG":
                    continue
                if WS_DEBUG and dbg < 5:
                    dbg += 1
                    log.info(f"[WS-RAW] {str(raw)[:300]}")
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                events = data if isinstance(data, list) else [data]
                touched = set()
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    et = ev.get("event_type", ev.get("type", ""))
                    tok = ev.get("asset_id", ev.get("assetId", ""))
                    if tok not in token_asset:
                        continue
                    if et == "book":
                        apply_book(tok, ev)
                        touched.add(token_asset[tok][0])
                    elif et == "price_change":
                        apply_change(tok, ev)
                        touched.add(token_asset[tok][0])
                    stats_events["n"] += 1
                for a in touched:
                    check_pair(a)
            close_all(time.time())
            try:
                ws.close()
            except Exception:
                pass
        except Exception as e:
            log.error(f"[WS] {e} — reconnecting")
            close_all(time.time())
            time.sleep(3)


def main():
    init_db()
    threading.Thread(target=ws_loop, daemon=True).start()
    tg(f"⚡ <b>PAIR-ARB WS census live</b> — no money\n"
       f"live book feed, ~0.1s detection, measures every cross's LIFETIME\n"
       f"edge ≥{PAIR_MIN_EDGE_CENTS:.1f}¢ · depth ≥{PAIR_MIN_DEPTH:.0f}/side · "
       f"tf={TF}m · tg only ≥{PAIR_TG_MIN_EDGE:.1f}¢\n"
       f"the number that gates a live test: crosses living 2s+\n/stats")
    hb = 0
    while True:
        try:
            handle_commands()
            if HEARTBEAT_SECS and time.time() - hb >= HEARTBEAT_SECS:
                hb = time.time()
                log.info(f"[Heartbeat] ws events={stats_events['n']} "
                         f"open episodes={len(active)}")
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
