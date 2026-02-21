import os
import json
import html
import requests
from datetime import datetime, timedelta, timezone

# ================================
# Constants
# ================================
JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"

# ================================
# Helpers
# ================================
def h(x):
    return html.escape(str(x), quote=True)

def to_f(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "N/A"

def fmt_pct(x):
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "N/A"

def _lower(s):
    return str(s or "").strip().lower()

def _to_ts_sec(ts):
    try:
        ts_i = int(ts)
        if ts_i > 10_000_000_000:
            ts_i //= 1000
        return ts_i
    except Exception:
        return None

def get_report_mode():
    return (os.getenv("REPORT_MODE") or "DAILY").strip().upper()

def get_period_end_jst():
    now = datetime.now(JST)
    end_dt = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < end_dt:
        end_dt -= timedelta(days=1)
    return end_dt

# ================================
# Telegram
# ================================
def send_telegram(text, chat_id):
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        print("TG_BOT_TOKEN missing")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    max_len = 3800
    chunks = []
    buf = ""

    for line in text.split("\n"):
        candidate = (buf + "\n" + line) if buf else line
        if len(candidate) > max_len:
            chunks.append(buf)
            buf = line
        else:
            buf = candidate

    if buf:
        chunks.append(buf)

    for chunk in chunks:
        r = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        if not r.ok:
            print("Telegram error:", r.text)
        r.raise_for_status()

# ================================
# Revert API
# ================================
def fetch_positions(safe, active=True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _normalize_positions(resp):
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        return resp.get("positions") or resp.get("data") or []
    return []

# ================================
# Net
# ================================
def calc_net_usd(pos):
    pooled = to_f(pos.get("underlying_value"))
    if pooled is None:
        return 0.0
    return pooled

# ================================
# Fee calc
# ================================
def calc_fees_in_window(pos_list_all, start_dt, end_dt):
    total = 0.0
    count = 0

    for pos in pos_list_all:
        cfs = pos.get("cash_flows") or []
        for cf in cfs:
            t = _lower(cf.get("type"))
            if t not in ("fees-collected", "claimed-fees"):
                continue

            ts = _to_ts_sec(cf.get("timestamp"))
            if ts is None:
                continue

            ts_dt = datetime.fromtimestamp(ts, JST)
            if ts_dt < start_dt or ts_dt >= end_dt:
                continue

            amt = to_f(cf.get("amount_usd"))
            if amt and amt > 0:
                total += amt
                count += 1

    return total, count

# ================================
# Daily
# ================================
# ================================
# Daily (FINAL layout: same as your "完成版イメージ")
# ================================
def build_daily_report_for_safe(safe: str) -> str:
    end_dt = get_period_end_jst()
    start_dt = end_dt - timedelta(days=1)

    pos_open = _normalize_positions(fetch_positions(safe, True))
    pos_exited = _normalize_positions(fetch_positions(safe, False))
    pos_all = (pos_open or []) + (pos_exited or [])

    # 24h fees (claimed)
    fee_24h, tx_24h = calc_fees_in_window(pos_all, start_dt, end_dt)

    # Net + Unclaimed (fees_value)
    net_total = 0.0
    unclaimed_total = 0.0
    for p in (pos_open or []):
        net_total += float(calc_net_usd(p) or 0.0)
        unclaimed_total += float(to_f(p.get("fees_value"), 0.0) or 0.0)

    apr_base = float(fee_24h or 0.0) + float(unclaimed_total or 0.0)
    safe_apr = (apr_base / net_total * 365 * 100) if net_total > 0 else 0.0

    sep = "────────────────"

    report = (
        "CBC Liquidity Mining — Daily\n\n"
        "SAFE\n"
        f"{h(safe)}\n\n"
        "Net合算\n"
        f"{fmt_money(net_total)}\n\n"
        f"{sep}\n"
        "推定総収益（24h＋未Claim）\n"
        f"{fmt_money(apr_base)}\n"
        f"{sep}\n\n"
        "確定手数料（24h）\n"
        f"{fmt_money(fee_24h)}\n\n"
        "蓄積手数料（未Claim）\n"
        f"{fmt_money(unclaimed_total)}\n\n"
        "Fee APR (SAFE)\n"
        f"{fmt_pct(safe_apr)}\n\n"
        "Transactions\n"
        f"{tx_24h}\n\n"
    )

    # NFT blocks
    for p in (pos_open or []):
        nft_id = str(p.get("nft_id", "UNKNOWN"))
        status = "OUT OF RANGE" if p.get("in_range") is False else "ACTIVE"

        net = float(calc_net_usd(p) or 0.0)
        fees_value = float(to_f(p.get("fees_value"), 0.0) or 0.0)
        nft_apr = (fees_value / net * 365 * 100) if net > 0 else 0.0

        nft_url = f"https://app.uniswap.org/positions/v3/base/{nft_id}"
        nft_link = f'<a href="{h(nft_url)}">{h(nft_id)}</a>'

        report += (
            f"{sep}\n"
            f"NFT {nft_link}\n"
            f"Status {h(status)}\n"
            f"{sep}\n\n"
            "Net\n"
            f"{fmt_money(net)}\n\n"
            "蓄積手数料（未Claim）\n"
            f"{fmt_money(fees_value)}\n\n"
            "Fee APR\n"
            f"{fmt_pct(nft_apr)}\n\n"
        )

    report += (
        "Period\n"
        f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}\n"
        "09:00 JST"
    )

    return report

# ================================
# Weekly
# ================================
def build_weekly_report_for_safe(safe):
    end_dt = get_period_end_jst()
    start_dt = end_dt - timedelta(days=7)

    pos_open = _normalize_positions(fetch_positions(safe, True))
    pos_exited = _normalize_positions(fetch_positions(safe, False))
    pos_all = pos_open + pos_exited

    fee_7d, tx_7d = calc_fees_in_window(pos_all, start_dt, end_dt)
    avg_per_day = fee_7d / 7.0 if fee_7d else 0.0

    net_total = sum(calc_net_usd(p) for p in pos_open)
    weekly_apr = (fee_7d / net_total * 52 * 100) if net_total > 0 else 0.0

    # ✅ All-time（2000年〜）
    fee_all_time, _ = calc_fees_in_window(
        pos_all,
        datetime(2000, 1, 1, tzinfo=JST),
        end_dt
    )

    sep = "———————"

    report = (
        "CBC Liquidity Mining — Weekly\n\n"
        "SAFE\n"
        f"{h(safe)}\n\n"
        f"{sep}\n"
        "7日確定手数料\n"
        f"{fmt_money(fee_7d)}\n"
        f"{sep}\n\n"
        "平均確定手数料\n"
        f"{fmt_money(avg_per_day)} / day\n\n"
        "Weekly APR\n"
        f"{fmt_pct(weekly_apr)}\n\n"
        "Transactions（7d）\n"
        f"{tx_7d}\n"
        f"{sep}\n"
        "累計確定（All-time）\n"
        f"{fmt_money(fee_all_time)}\n"
        f"{sep}\n\n"
        "Period\n"
        f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}\n"
        "09:00 JST"
    )

    return report

# ================================
# Main
# ================================
def load_config():
    path = os.getenv("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    mode = get_report_mode()
    cfg = load_config()
    safes = cfg.get("safes") or []

    for s in safes:
        name = s.get("name")
        safe = s.get("safe_address")
        chat_id = s.get("telegram_chat_id")

        if not safe or not chat_id:
            continue

        try:
            if mode == "WEEKLY":
                report = build_weekly_report_for_safe(safe)
            else:
                report = build_daily_report_for_safe(safe)

            send_telegram(report, chat_id)

        except Exception as e:
            print("ERROR:", e)
            send_telegram(
                f"CBC LM ERROR\nNAME: {h(name)}\nSAFE: {h(safe)}\n\n{h(e)}",
                chat_id
            )

if __name__ == "__main__":
    main()
