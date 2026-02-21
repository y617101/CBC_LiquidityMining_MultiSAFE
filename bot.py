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
# Mode / Time
# ================================
def get_report_mode() -> str:
    return (os.getenv("REPORT_MODE") or "DAILY").strip().upper()

def get_period_end_jst(now: datetime | None = None) -> datetime:
    """
    Period end aligned to 09:00 JST.
    - If now < 09:00 JST, end_dt = yesterday 09:00 JST
    - Else end_dt = today 09:00 JST
    """
    if now is None:
        now = datetime.now(JST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=JST)
        else:
            now = now.astimezone(JST)

    end_dt = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < end_dt:
        end_dt -= timedelta(days=1)
    return end_dt

# ================================
# Helpers
# ================================
def dbg(*args):
    if os.getenv("DEBUG") == "1":
        print(*args, flush=True)

def h(x) -> str:
    """HTML escape for Telegram parse_mode=HTML safety."""
    return html.escape(str(x), quote=True)

def to_f(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def fmt_money(x):
    try:
        if x is None:
            return "N/A"
        return f"${float(x):,.2f}"
    except Exception:
        return "N/A"

def fmt_pct(x):
    try:
        if x is None:
            return "N/A"
        return f"{float(x):.2f}%"
    except Exception:
        return "N/A"

def _lower(s):
    return str(s or "").strip().lower()

def _to_ts_sec(ts):
    try:
        ts_i = int(ts)
        if ts_i > 10_000_000_000:  # ms -> sec
            ts_i //= 1000
        return ts_i
    except Exception:
        return None

def _normalize_positions(resp) -> list[dict]:
    """
    Revert positions API sometimes returns:
    - list
    - dict with "positions"
    - dict with "data"
    """
    if isinstance(resp, list):
        return [p for p in resp if isinstance(p, dict)]
    if isinstance(resp, dict):
        data = resp.get("positions")
        if isinstance(data, list):
            return [p for p in data if isinstance(p, dict)]
        data = resp.get("data")
        if isinstance(data, list):
            return [p for p in data if isinstance(p, dict)]
    return []

def calc_fee_apr_a(fee_usd, net_usd):
    """
    APR% = fee_usd / net_usd * 365 * 100
    """
    try:
        if fee_usd is None or net_usd is None or float(net_usd) <= 0:
            return None
        return (float(fee_usd) / float(net_usd)) * 365 * 100
    except Exception:
        return None

# ================================
# Telegram
# ================================
def send_telegram(text: str, chat_id: str = None):
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        print("Telegram ENV missing: TG_BOT_TOKEN", flush=True)
        return

    chat_id = chat_id or os.getenv("TG_CHAT_ID")
    if not chat_id:
        print("Telegram ENV missing: TG_CHAT_ID", flush=True)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Telegram message limit ~4096 chars; keep margin
    max_len = 3800
    s = str(text)

    lines = s.split("\n")
    chunks = []
    buf = ""

    for line in lines:
        candidate = (buf + "\n" + line) if buf else line
        if len(candidate) > max_len:
            if buf:
                chunks.append(buf)
                buf = line
            else:
                chunks.append(line[:max_len])
                buf = line[max_len:]
        else:
            buf = candidate

    if buf:
        chunks.append(buf)

    for i, chunk in enumerate(chunks, 1):
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
        dbg(f"Telegram part {i}/{len(chunks)} status:", r.status_code)

        if not r.ok:
            # DEBUG時だけ返答本文も出す（400の理由が分かる）
            dbg("Telegram error body:", r.text)

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

# ================================
# Net USD
# ================================
def extract_repay_usd_from_cash_flows(pos):
    cfs = pos.get("cash_flows") or []
    if not isinstance(cfs, list):
        return 0.0

    # 1) total_debt を最優先（最新timestampのもの）
    latest = None
    latest_ts = -1
    for cf in cfs:
        if not isinstance(cf, dict):
            continue
        t = _lower(cf.get("type"))
        if t not in ("lendor-borrow", "lendor-repay"):
            continue
        td = to_f(cf.get("total_debt"))
        ts = to_f(cf.get("timestamp")) or 0
        if td is not None and ts >= latest_ts:
            latest_ts = ts
            latest = td

    if latest is not None:
        try:
            return max(float(latest), 0.0)
        except Exception:
            return 0.0

    # 2) total_debt が無い場合のみ、borrows - repays をUSDで集計
    borrow_usd = 0.0
    repay_usd = 0.0

    for cf in cfs:
        if not isinstance(cf, dict):
            continue
        t = _lower(cf.get("type"))
        if t not in ("lendor-borrow", "lendor-repay"):
            continue

        v = to_f(cf.get("amount_usd"))
        if v is None:
            v = to_f(cf.get("usd"))
        if v is None:
            v = to_f(cf.get("value_usd"))
        if v is None:
            v = to_f(cf.get("valueUsd"))
        if v is None:
            v = to_f(cf.get("amountUsd"))

        if v is None:
            continue

        v = abs(float(v))
        if t == "lendor-borrow":
            borrow_usd += v
        else:
            repay_usd += v

    debt = borrow_usd - repay_usd
    return debt if debt > 0 else 0.0

def calc_net_usd(pos):
    pooled_usd = to_f(pos.get("underlying_value"))
    if pooled_usd is None:
        return None
    debt_usd = extract_repay_usd_from_cash_flows(pos) or 0.0
    return pooled_usd - debt_usd

# ================================
# Fee calc (DAILY 24h window) — by NFT + total
# ================================
def calc_fee_usd_24h_from_cash_flows(pos_list_all, now_dt: datetime):
    """
    24h window aligned to 09:00 JST:
      start_dt = end_dt - 1 day
      end_dt   = aligned period end
    Collect types: fees-collected / claimed-fees
    - If amount_usd is None, reconstruct from token amounts * prices.
    Returns:
      (total_usd, total_count, fee_by_nft, count_by_nft, start_dt, end_dt)
    """
    end_dt = get_period_end_jst(now_dt)
    start_dt = end_dt - timedelta(days=1)

    total = 0.0
    total_count = 0
    fee_by_nft: dict[str, float] = {}
    count_by_nft: dict[str, int] = {}

    for pos in (pos_list_all or []):
        if not isinstance(pos, dict):
            continue

        nft_id = str(pos.get("nft_id", "UNKNOWN"))

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

            # amount_usd が無い場合は、数量×価格で復元
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

                amt_usd = abs(q0) * p0 + abs(q1) * p1

            try:
                amt_usd = float(amt_usd)
            except Exception:
                continue

            if amt_usd <= 0:
                continue

            total += amt_usd
            total_count += 1

            fee_by_nft[nft_id] = float(fee_by_nft.get(nft_id, 0.0) or 0.0) + amt_usd
            count_by_nft[nft_id] = int(count_by_nft.get(nft_id, 0) or 0) + 1

    return total, total_count, fee_by_nft, count_by_nft, start_dt, end_dt

# ================================
# Fee calc (WEEKLY window)
# ================================
def calc_fees_usd_in_window_from_cash_flows(pos_list_all, start_dt: datetime, end_dt: datetime):
    """
    Sum fees (USD) in [start_dt, end_dt) from positions cash_flows.
    Target types: fees-collected / claimed-fees
    - If amount_usd is None, reconstruct from token amounts * prices.
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

                amt_usd = abs(q0) * p0 + abs(q1) * p1

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
# config
# ================================
def load_config():
    path = os.environ.get("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ================================
# Daily
# ================================
def build_daily_report_for_safe(safe: str) -> str:
    positions_open = fetch_positions(safe, active=True)
    positions_exited = fetch_positions(safe, active=False)

    pos_list_open = _normalize_positions(positions_open)
    pos_list_exited = _normalize_positions(positions_exited)

    pos_list_all = []
    pos_list_all += pos_list_open
    pos_list_all += pos_list_exited

    now_dt = datetime.now(JST)
    fee_usd, fee_count, fee_by_nft, _, _, end_dt = calc_fee_usd_24h_from_cash_flows(pos_list_all, now_dt)

    nft_lines = []
    net_total = 0.0
    unclaimed_total = 0.0  # fees_value合算(USD)

    for pos in pos_list_open:
        nft_id = str(pos.get("nft_id", "UNKNOWN"))

        in_range = pos.get("in_range")
        status = "OUT OF RANGE" if in_range is False else "ACTIVE"

        net = float(calc_net_usd(pos) or 0.0)
        net_total += net

        fees_value = float(to_f(pos.get("fees_value"), 0.0) or 0.0)
        unclaimed_total += fees_value

        fee_usd_nft = float(fee_by_nft.get(nft_id, 0.0) or 0.0)

        # NFT APR: (24h確定 + 未Claim) / Net * 365
        nft_apr_base = fee_usd_nft + fees_value
        nft_fee_apr = calc_fee_apr_a(nft_apr_base, net)

        nft_url = f"https://app.uniswap.org/positions/v3/base/{nft_id}"
        nft_link = f'<a href="{h(nft_url)}">{h(nft_id)}</a>'

        nft_lines.append(
            "\n"
            f"NFT {nft_link}\n"
            f"Status: {h(status)}\n"
            f"Net: {fmt_money(net)}\n"
            f"蓄積手数料（未Claim） {fmt_money(fees_value)}\n"
            f"Fee APR: {fmt_pct(nft_fee_apr)}\n"
        )

    # SAFE APR: (24h確定 + 未Claim合計) / Net合算 * 365
    apr_base_usd = float(fee_usd or 0.0) + float(unclaimed_total or 0.0)
    safe_fee_apr = calc_fee_apr_a(apr_base_usd, net_total)

    report = (
        "CBC Liquidity Mining — Daily\n"
        f"Period End: {end_dt.strftime('%Y-%m-%d %H:%M')} JST\n"
        "────────────────\n"
        f"SAFE\n{h(safe)}\n\n"
        f"・推定総収益（24h＋未Claim） {fmt_money(apr_base_usd)}\n"
        f"・確定手数料（24h） {fmt_money(fee_usd)}\n"
        f"・蓄積手数料（未Claim） {fmt_money(unclaimed_total)}\n"
        f"・Fee APR(SAFE) {fmt_pct(safe_fee_apr)}\n\n"
        f"・Net合算 {fmt_money(net_total)}\n"
        f"・Transactions {fee_count}\n"
        + "".join(nft_lines)
    )

    return report

# ===============================
# Weekly (FINAL layout)
# ===============================
def build_weekly_report_for_safe(safe: str) -> str:
    end_dt = get_period_end_jst()
    start_dt = end_dt - timedelta(days=7)

    positions_open = fetch_positions(safe, active=True)
    positions_exited = fetch_positions(safe, active=False)

    pos_list_open = _normalize_positions(positions_open)
    pos_list_exited = _normalize_positions(positions_exited)

    pos_list_all = []
    pos_list_all += (pos_list_open or [])
    pos_list_all += (pos_list_exited or [])

    # 7d fees
    fee_7d_usd, tx_7d = calc_fees_usd_in_window_from_cash_flows(pos_list_all, start_dt, end_dt)
    avg_per_day = (fee_7d_usd / 7.0) if fee_7d_usd else 0.0

    # all-time fees（2000年〜）
    fee_all_time_usd, _ = calc_fees_usd_in_window_from_cash_flows(
        pos_list_all,
        datetime(2000, 1, 1, tzinfo=JST),
        end_dt,
    )

    # Net合算（APR分母：openのみ）
    net_total = 0.0
    for pos in (pos_list_open or []):
        net_total += float(calc_net_usd(pos) or 0.0)

    weekly_apr_pct = (fee_7d_usd / net_total) * 52 * 100 if net_total > 0 else 0.0

    report = (
        "CBC Liquidity Mining — Weekly\n\n"
        "SAFE\n"
        f"{h(safe)}\n\n\n"
        "7日確定手数料\n"
        f"{fmt_money(fee_7d_usd)}\n\n"
        "平均確定手数料\n"
        f"{fmt_money(avg_per_day)} / day\n\n"
        "Weekly APR\n"
        f"{fmt_pct(weekly_apr_pct)}\n\n"
        "Transactions（7d）\n"
        f"{tx_7d}\n\n"
        "累計確定（All-time）\n"
        f"{fmt_money(fee_all_time_usd)}\n\n\n"
        "Period\n"
        f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}\n"
        "09:00 JST"
    )

    return report

# ===============================
# main
# ===============================
def main():
    mode = get_report_mode()
    print(f"DBG REPORT_MODE={mode}", flush=True)

    cfg = load_config()
    safes = cfg.get("safes") or []
    if not safes:
        print("config: safes is empty", flush=True)
        return

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
            print(f"error name={name} safe={safe}: {e}", flush=True)
            # エラー通知（短め）
            try:
                send_telegram(
                    f"CBC LM ERROR\n"
                    f"NAME: {h(name)}\n"
                    f"SAFE: {h(safe)}\n\n"
                    f"{h(e)}",
                    chat_id,
                )
            except Exception:
                pass

if __name__ == "__main__":
    main()
