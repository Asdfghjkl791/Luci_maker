#!/usr/bin/env python3
# LP REWARDS CENSUS — Phase A of the supply-side question (no money)
#
# THE QUESTION
#   Polymarket's 2026 incentive layer pays makers daily for resting two-sided
#   limit orders near the midpoint. Pools, qualifying spread, and minimum size
#   are PUBLIC per market. This census answers, for the 5m/15m crypto markets:
#     1. Do these markets carry reward pools at all, and how big?
#     2. How much qualifying liquidity already competes inside the spread?
#     3. What would a $500-$1000 two-sided quoter's GROSS share be per day —
#        and does it clear the $1/day/market minimum-payout floor?
#
#   INCOME PIPE ONLY. Deliberately excludes fill toxicity (Phase B's job).
#   Pre-registered bar: gross >= $5/day at $750 or this door closes finally.
#
# FIELD-NAME HEDGE: rewards config field names on the Gamma/CLOB market objects
#   are read defensively (multiple known spellings) and RAW_DEBUG=true dumps a
#   full market object once so the parser can be corrected from the wire.
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): DB_PATH, CAPITAL_USD=750, SAMPLE_SECS=60, TIMEFRAMES=5,15,
#   RAW_DEBUG=false

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
DB_PATH     = os.environ.get("DB_PATH", "rewards_census.db")
CAPITAL_USD = float(os.environ.get("CAPITAL_USD", "750"))
SAMPLE_SECS = float(os.environ.get("SAMPLE_SECS", "60"))
TFS         = [int(x) for x in os.environ.get("TIMEFRAMES", "5,15").split(",")]
RAW_DEBUG   = os.environ.get("RAW_DEBUG", "false").lower() == "true"

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rewards-census")

_dumped = {"n": 0}
_rw_cache = {}


def _clob_rewards(condition_id):
    """Rewards object from the CLOB market endpoint — the daily pool lives HERE,
    not on the Gamma object (verified from a live RAW-MARKET dump). Cached."""
    if condition_id in _rw_cache:
        return _rw_cache[condition_id]
    rw = None
    try:
        r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
        j = r.json()
        rw = j.get("rewards") if isinstance(j, dict) else None
        if RAW_DEBUG and _dumped["n"] < 6:
            _dumped["n"] += 1
            log.info(f"[RAW-CLOB-REWARDS] {json.dumps(rw)[:400]}")
    except Exception as e:
        log.debug(f"[CLOB-RW] {str(condition_id)[:12]}: {e}")
    if len(_rw_cache) > 4000:
        _rw_cache.clear()
    _rw_cache[condition_id] = rw
    return rw


def window_times(tf):
    now = int(time.time())
    L = tf * 60
    o = (now // L) * L
    return o, o + L, o + L - now


def fetch_market(asset, tf, open_ts):
    """Full market object (tokens + rewards config), defensively parsed."""
    slug = f"{asset.lower()}-updown-{tf}m-{open_ts}"
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=8)
        arr = r.json()
        ev = arr[0] if isinstance(arr, list) and arr else arr
        markets = ev.get("markets", []) if isinstance(ev, dict) else []
        if not markets:
            return None
        m = markets[0]
        if RAW_DEBUG and _dumped["n"] < 2:
            _dumped["n"] += 1
            log.info(f"[RAW-MARKET] keys={sorted(m.keys())}")
            log.info(f"[RAW-MARKET] {json.dumps(m)[:1200]}")
        toks = json.loads(m.get("clobTokenIds", "[]"))
        if len(toks) != 2:
            return None
        # rewards config — multiple known spellings, else None
        def num(*keys):
            for k in keys:
                v = m.get(k)
                if v not in (None, "", "0", 0):
                    try:
                        return float(v)
                    except Exception:
                        pass
            return None
        min_size  = num("rewardsMinSize", "rewards_min_size", "minIncentiveSize")
        max_sprd  = num("rewardsMaxSpread", "rewards_max_spread",
                        "maxIncentiveSpread")
        # Daily pool comes from the CLOB market endpoint (rewards.rates[]);
        # the Gamma object has no rewards-amount key at all.
        pool = None
        cid = m.get("conditionId") or m.get("condition_id")
        if cid:
            rw = _clob_rewards(cid)
            if rw:
                for entry in (rw.get("rates") or []):
                    if isinstance(entry, dict):
                        v = (entry.get("rewards_daily_rate")
                             or entry.get("rewardsDailyRate"))
                        if v:
                            try:
                                pool = (pool or 0) + float(v)
                            except Exception:
                                pass
                for k, cur in (("min_size", min_size), ("max_spread", max_sprd)):
                    if cur is None and rw.get(k) not in (None, ""):
                        try:
                            if k == "min_size":
                                min_size = float(rw[k])
                            else:
                                max_sprd = float(rw[k])
                        except Exception:
                            pass
        return {"tokens": toks, "min_size": min_size,
                "max_spread": max_sprd, "pool": pool}
    except Exception as e:
        log.debug(f"[MKT] {slug}: {e}")
        return None


def fetch_book(token_id):
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id},
                         timeout=6)
        b = r.json()
        bids = [(float(x["price"]), float(x.get("size", 0)))
                for x in b.get("bids", []) if float(x.get("size", 0)) > 0]
        asks = [(float(x["price"]), float(x.get("size", 0)))
                for x in b.get("asks", []) if float(x.get("size", 0)) > 0]
        return bids, asks
    except Exception:
        return None


def qscore(spread, max_spread, size):
    """Polymarket's published per-order score: ((S - s)/S)^2 * size."""
    if max_spread is None or max_spread <= 0 or spread > max_spread:
        return 0.0
    return ((max_spread - spread) / max_spread) ** 2 * size


def sample_market(asset, tf, mk):
    """One census sample: competing score vs our hypothetical score -> share."""
    up = fetch_book(mk["tokens"][0])
    if not up:
        return None
    bids, asks = up
    if not bids or not asks:
        return None
    mid = (max(p for p, _ in bids) + min(p for p, _ in asks)) / 2.0
    ms = mk["max_spread"]
    if ms is None:
        # market carries NO rewards config — record that as a real finding
        # (pool 0), so "no pools exist" is distinguishable from "sampler dead"
        return {"pool": 0.0, "share": 0.0, "min_ok": False,
                "comp_bid_sz": sum(sz for _, sz in bids),
                "comp_ask_sz": sum(sz for _, sz in asks)}
    ms_frac = ms / 100.0 if ms > 1 else ms   # cents vs fraction, defensively
    # competing qualifying score, each side (UP token book == both sides of mkt)
    comp_bid = sum(qscore(abs(mid - p), ms_frac, sz) for p, sz in bids)
    comp_ask = sum(qscore(abs(p - mid), ms_frac, sz) for p, sz in asks)
    # two-sided min rule (single-sided allowed at /c=3 only if mid in [.1,.9])
    if 0.10 <= mid <= 0.90:
        comp = max(min(comp_bid, comp_ask), max(comp_bid, comp_ask) / 3.0)
    else:
        comp = min(comp_bid, comp_ask)
    # OUR hypothetical quote: both sides at the inside (spread ≈ half the
    # bid-ask), sized by capital split across markets
    n_mk = max(1, len(TFS) * len(ASSET_LIST))
    cap_here = CAPITAL_USD / n_mk
    our_shares = (cap_here / 2.0) / max(mid, 0.02)   # per side
    min_ok = (mk["min_size"] is None) or (our_shares >= mk["min_size"])
    inside = abs(min(p for p, _ in asks) - mid)
    ours = qscore(inside, ms_frac, our_shares)
    if 0.10 <= mid <= 0.90:
        our_q = ours          # two-sided symmetric
    else:
        our_q = ours          # must be two-sided anyway; same size both sides
    share = our_q / (comp + our_q) if (comp + our_q) > 0 else 0.0
    return {"pool": mk["pool"] or 0.0, "share": share, "min_ok": min_ok,
            "comp_bid_sz": sum(sz for _, sz in bids),
            "comp_ask_sz": sum(sz for _, sz in asks)}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, asset TEXT,
        tf INTEGER, pool REAL, share REAL, min_ok INTEGER,
        depth_bid REAL, depth_ask REAL)""")
    conn.commit()
    conn.close()


def db_add(asset, tf, s):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO samples (created,asset,tf,pool,share,min_ok,"
                 "depth_bid,depth_ask) VALUES (?,?,?,?,?,?,?,?)",
                 (datetime.now(timezone.utc).isoformat(), asset, tf,
                  s["pool"], s["share"], 1 if s["min_ok"] else 0,
                  s["comp_bid_sz"], s["comp_ask_sz"]))
    conn.commit()
    conn.close()


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
            log.error(f"[TG] REJECTED {getattr(r,'status_code','?')}: "
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
                c.execute("SELECT asset, tf, pool, share, min_ok FROM samples")
                rows = c.fetchall()
                conn.close()
                if not rows:
                    tg("💰 <b>REWARDS census</b>\nno samples yet")
                    continue
                pools = [r[2] for r in rows if r[2]]
                if not pools:
                    tg("💰 <b>REWARDS census</b>\n"
                       f"{len(rows)} samples · <b>NO reward pools found on these "
                       f"markets</b>\nIf this holds for 24h, the crypto updown "
                       f"markets carry no LP pools and Phase A fails on "
                       f"question 1.")
                    continue
                per = {}
                for a, tf, pool, share, mok in rows:
                    k = f"{a} {tf}m"
                    per.setdefault(k, []).append((pool or 0, share, mok))
                lines = []
                total = 0.0
                for k, v in sorted(per.items()):
                    ap = sum(x[0] for x in v) / len(v)
                    ash = sum(x[1] for x in v) / len(v)
                    mokr = sum(x[2] for x in v) / len(v)
                    g = ap * ash
                    floor = "" if g >= 1.0 else " &lt;$1 floor ⚠️"
                    minw = "" if mokr > 0.5 else " size&lt;min ❌"
                    if g >= 1.0 and mokr > 0.5:
                        total += g
                    lines.append(f"{k}: pool ~${ap:.0f}/d · share {ash*100:.2f}% "
                                 f"→ ${g:.2f}/d{floor}{minw}")
                verdict = ("✅ ABOVE the $5/day bar" if total >= 5
                           else "⚠️ below the $5/day bar")
                tg(f"💰 <b>REWARDS census</b> @ ${CAPITAL_USD:.0f}\n"
                   + "\n".join(lines[:14]) +
                   f"\n<b>payable gross ≈ ${total:.2f}/day</b> "
                   f"(after $1 floors + min-size gates)\n{verdict}")
    except Exception:
        pass


def sampler():
    while True:
        try:
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                if secs_left < 20:
                    continue
                for asset in ASSET_LIST:
                    mk = fetch_market(asset, tf, open_ts)
                    if not mk:
                        continue
                    s = sample_market(asset, tf, mk)
                    if s:
                        db_add(asset, tf, s)
                        if s["pool"]:
                            log.info(f"[SAMPLE] {asset} {tf}m pool ${s['pool']:.0f}/d "
                                     f"share {s['share']*100:.2f}% min_ok={s['min_ok']}")
                        else:
                            log.info(f"[SAMPLE] {asset} {tf}m NO POOL")
        except Exception as e:
            log.error(f"[SAMPLER] {e}")
        time.sleep(SAMPLE_SECS)


def main():
    init_db()
    threading.Thread(target=sampler, daemon=True).start()
    tg(f"💰 <b>LP REWARDS CENSUS live</b> — no money, Phase A\n"
       f"question 1: do the crypto updown markets carry reward pools?\n"
       f"question 2: what would ${CAPITAL_USD:.0f} two-sided earn GROSS/day?\n"
       f"pre-registered bar: ≥$5/day payable gross, else this closes\n/stats")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
