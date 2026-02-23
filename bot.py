import os
import json
import html
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import gspread
from google.oauth2.service_account import Credentials

JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"

# =====================================================
# TIME (09:00 JST strict anchor)
# =====================================================
def get_period_end_jst(now=None):
    now = now or datetime.now(JST)
    now = now.replace(tzinfo=JST) if now.tzinfo is None else now.astimezone(JST)
    anchor = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=1)
    return anchor

# =====================================================
# FORMAT
# =====================================================
def fmt_money(x): return f"${float(x):,.2f}"
def fmt_pct(x): return f"{float(x):.2f}%"
def mask_safe(addr): return f"{addr[:7]}*****{addr[-4:]}" if len(addr)>11 else addr
def nft_link(n): return f'<a href="https://app.uniswap.org/positions/v3/base/{n}">{n}</a>'

# =====================================================
# REVERT API
# =====================================================
def fetch_positions(safe, active=True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("positions", [])

# =====================================================
# NET
# =====================================================
def calc_net(pos):
    return float(pos.get("underlying_value") or 0)

# =====================================================
# APR
# =====================================================
def nft_apr(pos): return float(pos.get("fee_apr") or 0)

def safe_apr_weighted(pos_list):
    total_net = 0
    weighted = 0
    for p in pos_list:
        net = calc_net(p)
        apr = nft_apr(p)
        if net <= 0:
            continue
        total_net += net
        weighted += apr * net
    return weighted / total_net if total_net else 0

# =====================================================
# CLAIMED (重複防止)
# =====================================================
def claimed_window(pos_list, start_dt, end_dt):
    total = 0
    seen = set()

    for p in pos_list:
        nft_id = p.get("nft_id")
        for cf in p.get("cash_flows", []):
            t = str(cf.get("type","")).lower()
            if t not in ("fees-collected","claimed-fees"):
                continue

            ts = int(cf.get("timestamp",0))
            if ts > 10_000_000_000:
                ts //= 1000

            dt = datetime.fromtimestamp(ts, JST)
            if dt < start_dt or dt >= end_dt:
                continue

            key = (cf.get("tx_hash"), t, nft_id)
            if key in seen:
                continue
            seen.add(key)

            total += float(cf.get("amount_usd") or 0)

    return total

# =====================================================
# SHEETS ENGINE
# =====================================================
class SheetEngine:

    def __init__(self):
        creds = Credentials.from_service_account_file(
            "gcp_service_account.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
        self.log_ws = self.sh.worksheet("DAILY_LOG")
        self.wide_ws = self.sh.worksheet("DAILY_WIDE")

    # ---------- DAILY_LOG (縦) ----------
    def get_log_rows(self):
        return self.log_ws.get_all_values()

    def upsert_log(self, period_end, safe_name, safe, net, claimed, unclaimed, emitted):
        key = period_end.strftime("%Y-%m-%d %H:%M")
        rows = self.get_log_rows()

        for i, r in enumerate(rows):
            if len(r)>=3 and r[0]==key and r[2].lower()==safe.lower():
                self.log_ws.update(
                    f"A{i+1}:G{i+1}",
                    [[key, safe_name, safe, net, claimed, unclaimed, emitted]]
                )
                return

        self.log_ws.append_row([key, safe_name, safe, net, claimed, unclaimed, emitted])

    def get_yesterday_unclaimed(self, safe):
        rows = self.get_log_rows()
        hist = [r for r in rows if len(r)>=6 and r[2].lower()==safe.lower()]
        if len(hist)>=2:
            return float(hist[-2][5])
        return 0

    # ---------- DAILY_WIDE (横) ----------
    def update_wide(self, period_end, safe_name, safe, claimed):
        key = period_end.strftime("%Y-%m-%d %H:%M")
        rows = self.wide_ws.get_all_values()

        if not rows:
            self.wide_ws.append_row(["period_end_jst", safe_name])
            self.wide_ws.append_row(["safe_address", safe])
            self.wide_ws.append_row([key, claimed])
            return

        header = rows[0]
        if safe_name not in header:
            header.append(safe_name)
            self.wide_ws.update("A1", [header])
            rows = self.wide_ws.get_all_values()

        col = header.index(safe_name) + 1

        row_idx = None
        for i,r in enumerate(rows):
            if r and r[0]==key:
                row_idx=i+1
                break

        if row_idx is None:
            self.wide_ws.append_row([key])
            rows = self.wide_ws.get_all_values()
            row_idx=len(rows)

        self.wide_ws.update_cell(row_idx, col, claimed)

# =====================================================
# DAILY
# =====================================================
def run_daily(safe_name, safe, chat_id, sheets):
    period_end = get_period_end_jst()
    start = period_end - timedelta(days=1)

    pos_open = fetch_positions(safe, True)
    pos_all = pos_open + fetch_positions(safe, False)

    net_total = sum(calc_net(p) for p in pos_open)
    claimed = claimed_window(pos_all, start, period_end)

    unclaimed = sum(float(p.get("fees_value") or 0) for p in pos_open)
    y_unclaimed = sheets.get_yesterday_unclaimed(safe)
    delta = max(0, unclaimed - y_unclaimed)
    emitted = claimed + delta

    sheets.upsert_log(period_end, safe_name, safe, net_total, claimed, unclaimed, emitted)
    sheets.update_wide(period_end, safe_name, safe, claimed)

    apr = safe_apr_weighted(pos_open)

    msg = (
        "🚀 CBC Liquidity Mining — Daily\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"SAFE {mask_safe(safe)}\n"
        "────────────────\n\n"
        "📈 推定戦略 APR\n"
        f"{fmt_pct(apr)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "🎉 当日DEX手数料収益\n"
        f"{fmt_money(emitted)}\n\n"
        "📊 NFT Positions\n"
    )

    for p in pos_open:
        msg += (
            f"{nft_link(p['nft_id'])} | "
            f"{'OUT OF RANGE' if p.get('in_range') is False else 'ACTIVE'} | "
            f"Net {fmt_money(calc_net(p))} | "
            f"APR {fmt_pct(nft_apr(p))}\n"
        )

    send_telegram(msg, chat_id)

# =====================================================
# WEEKLY
# =====================================================
def run_weekly(safe_name, safe, chat_id):
    period_end = get_period_end_jst()
    start_this = period_end - timedelta(days=7)
    start_prev = period_end - timedelta(days=14)
    end_prev = period_end - timedelta(days=7)

    pos_open = fetch_positions(safe, True)
    pos_all = pos_open + fetch_positions(safe, False)

    week = claimed_window(pos_all, start_this, period_end)
    prev = claimed_window(pos_all, start_prev, end_prev)

    net_total = sum(calc_net(p) for p in pos_open)
    apr = safe_apr_weighted(pos_open)

    msg = (
        "🚀 CBC Liquidity Mining — Weekly Settlement\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"SAFE {mask_safe(safe)}\n"
        "────────────────\n\n"
        "🎉 今週確定収益\n"
        f"{fmt_money(week)}\n\n"
        "📈 推定戦略 APR\n"
        f"{fmt_pct(apr)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "📊 NFT Positions\n"
    )

    for p in pos_open:
        msg += (
            f"{nft_link(p['nft_id'])} | "
            f"{'OUT OF RANGE' if p.get('in_range') is False else 'ACTIVE'} | "
            f"Net {fmt_money(calc_net(p))} | "
            f"APR {fmt_pct(nft_apr(p))}\n"
        )

    send_telegram(msg, chat_id)

# =====================================================
# TELEGRAM
# =====================================================
def send_telegram(text, chat_id):
    token = os.getenv("TG_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=30).raise_for_status()

# =====================================================
# MAIN
# =====================================================
def main():
    mode = os.getenv("REPORT_MODE","").upper()
    if mode not in ("DAILY","WEEKLY"):
        mode = "WEEKLY" if datetime.now(JST).weekday()==6 else "DAILY"

    cfg = json.load(open("config.json"))
    sheets = SheetEngine()

    for s in cfg["safes"]:
        safe_name = s["name"]
        safe = s["safe_address"]
        chat_id = s["telegram_chat_id"]

        if mode == "DAILY":
            run_daily(safe_name, safe, chat_id, sheets)
        else:
            run_weekly(safe_name, safe, chat_id)

if __name__ == "__main__":
    main()
