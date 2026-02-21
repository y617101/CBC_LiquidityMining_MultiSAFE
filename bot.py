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
def h(x) -> str:
    return html.escape(str(x), quote=True)

def to_f(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def fmt_money(x) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "N/A"

def fmt_pct(x) -> str:
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "N/A"

def _lower(s) -> str:
    return str(s or "").strip().lower()

def _to_ts_sec(ts):
    try:
        ts_i = int(ts)
        if ts_i > 10_000_000_000:  # ms -> sec
            ts_i //= 1000
        return ts_i
    except Exception:
        return None

def get_report_mode() -> str:
    return (os.getenv("REPORT_MODE") or "DAILY").strip().upper()

def get_period_end_jst() -> datetime:
    """
    Period end aligned to 09:00 JST.
    - If now < 09:00 JST, end_dt = yesterday 09:00 JST
    - Else end_dt = today 09:00 JST
    """
    now = datetime.now(JST)
    end_dt = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < end_dt:
        end_dt -= timedelta(days=1)
    return end_dt

# ================================
# Telegram
# ================================
def send_telegram(text: str, chat_id: str):
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        print("TG_BOT_TOKEN missing", flush=True)
        return
    if not chat_id:
        print("chat_id missing", flush=True)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Telegram hard limit is 4096; keep margin
    max_len = 3800

    s = str(text or "")
    lines = s.split("\n")

    chunks = []
    buf = ""

    for line in lines:
        candidate = (buf + "\n" + line) if buf else line
        if len(candidate) > max_len:
            # flush current buffer if exists
            if buf:
                chunks.append(buf)
                buf = line
            else:
                # single very long line: split hard
                chunks.append(line[:max_len])
                buf = line[max_len:]
        else:
            buf = candidate

    if buf:
        chunks.append(buf)

    for chunk in chunks:
        if not chunk.strip():
            continue  # avoid empty messages that can cause 400
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
            print("Telegram error:", r.status_code, r.text, flush=True)
        r.raise_for_status()

# ================================
# Revert API
# ================================
def fetch_positions(safe: str, active: bool = True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _normalize_positions(resp) -> list[dict]:
    if isinstance(resp, list):
        return [p for p in resp if isinstance(p, dict)]
    if isinstance(resp, dict):
        data = resp.get("positions") or resp.get("data") or []
        if isinstance(data, list):
            return [p for p in data if isinstance(p, dict)]
    return []

# ================================
# Net (placeholder: you will finalize later)
# ================================
def calc_net_usd(pos) -> float:
    """
    NOTE: Placeholder Net.
    Currently returns underlying_value only.
    You will replace this with: Net = pooled assets - debt (repay/total_debt logic).
    """
    pooled = to_f(pos.get("underlying_value"))
    if pooled is None:
        return 0.0
    return float(pooled)

# ================================
# Fee calc (key fix: reconstruct when amount_usd is None)
# ================================
def calc_fees_in_window(pos_list_all, start_dt: datetime, end_dt: datetime):
    """
    Sum fees (USD) in [start_dt, end_dt) from positions cash_flows.
    Target types: fees-collected / claimed-fees

    If amount_usd is None:
      reconstruct from (token amounts) * (prices.token0.usd / prices.token1.usd).

    Returns: (total_usd, tx_count)
    """
    total = 0.0
    count = 0

    for pos in (pos_list_all or []):
        if not isinstance(pos, dict):
            continue

        cfs = pos.get("cash_flows") or []
        if not isinstance(cfs, list):
            continue

        for cf in cfs:
            if not isinstance(cf, dict):
                continue

            t = _lower(cf.get("type"))
            if t not in ("fees-collected", "claimed-fees"):
                continue

            ts = _to_ts_sec(cf.get("timestamp"))
            if ts is None:
                continue

            ts_dt = datetime.fromtimestamp(ts, JST)
            if ts_dt < start_dt or ts_dt >= end_dt:
                continue

            amt_usd = to_f(cf.get("amount_usd"))

            # reconstruct when amount_usd missing
            if amt_usd is None:
                prices = cf.get("prices") or {}
                p0 = to_f((prices.get("token0") or {}).get("usd")) or 0.0
                p1 = to_f((prices.get("token1") or {}).get("usd")) or 0.0

                q0 = (
                    to_f(cf.get("collected_fees_token0"))
                    or to_f(cf.get("claimed_token0"))
                    or to_f(cf.get("fees0"))
                    or to_f(cf.get("amount0"))
                    or 0.0
                )
                q1 = (
                    to_f(cf.get("collected_fees_token1"))
                    or to_f(cf.get("claimed_token1"))
                    or to_f(cf.get("fees1"))
                    or to_f(cf.get("amount1"))
                    or 0.0
                )

                amt_usd = abs(float(q0)) * float(p0) + abs(float(q1)) * float(p1)

            try:
                amt_usd = float(amt_usd)
            except Exception:
                continue

            if amt_usd <= 0:
                continue

            total += amt_usd
            count += 1

    return total, count

# ================================
# Daily (FINAL layout: same as your image)
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

        # NOTE: NFT APR here is "unclaimed-only". If you want (24h claimed + unclaimed) per NFT,
        # we can add fee_24h_by_nft later. For now keep consistent and stable.
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
# Weekly (your separator style)
# ================================
def build_weekly_report_for_safe(safe: str) -> str:
    end_dt = get_period_end_jst()
    start_dt = end_dt - timedelta(days=7)

    pos_open = _normalize_positions(fetch_positions(safe, True))
    pos_exited = _normalize_positions(fetch_positions(safe, False))
    pos_all = (pos_open or []) + (pos_exited or [])

    fee_7d, tx_7d = calc_fees_in_window(pos_all, start_dt, end_dt)
    avg_per_day = fee_7d / 7.0 if fee_7d else 0.0

    net_total = sum(float(calc_net_usd(p) or 0.0) for p in (pos_open or []))
    weekly_apr = (fee_7d / net_total * 52 * 100) if net_total > 0 else 0.0

    # All-time（2000年〜）
    fee_all_time, _ = calc_fees_in_window(
        pos_all,
        datetime(2000, 1, 1, tzinfo=JST),
        end_dt,
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
        name = s.get("name") or "NONAME"
        safe = s.get("safe_address")
        chat_id = s.get("telegram_chat_id")

        if not safe or not chat_id:
            print(f"skip: missing safe/chat_id name={name}", flush=True)
            continue

        try:
            if mode == "WEEKLY":
                report = build_weekly_report_for_safe(safe)
            else:
                report = build_daily_report_for_safe(safe)

            send_telegram(report, chat_id)

        except Exception as e:
            print(f"ERROR name={name} safe={safe}: {e}", flush=True)
            try:
                send_telegram(
                    f"CBC LM ERROR\nNAME: {h(name)}\nSAFE: {h(safe)}\n\n{h(e)}",
                    chat_id,
                )
            except Exception:
                pass

if __name__ == "__main__":
    main()
