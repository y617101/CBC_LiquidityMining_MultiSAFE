import os

def get_report_mode() -> str:
    return (os.getenv("REPORT_MODE") or "DAILY").strip().upper()
import json
import html
import requests
from datetime import datetime, timedelta, timezone

# ================================
# Token Symbol Map (Base)
# ================================
ADDRESS_SYMBOL_MAP = {
    # Base canonical WETH
    "0x4200000000000000000000000000000000000006": "WETH",
    # Base USDC
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
}

JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"


# ----------------
# helpers
# ----------------
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
    return "N/A" if x is None else f"${x:,.2f}"


def fmt_pct(x):
    return "N/A" if x is None else f"{x:.2f}%"


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


def calc_fee_apr_a(fee_24h_usd, net_usd):
    if fee_24h_usd is None or net_usd is None or net_usd <= 0:
        return None
    return (fee_24h_usd / net_usd) * 365 * 100


# ----------------
# Telegram
# ----------------
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

    # Telegram message limit is ~4096 chars; keep margin
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
        r.raise_for_status()


# ----------------
# Revert API
# ----------------
def fetch_positions(safe: str, active: bool = True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def resolve_symbol(pos, which):
    v = pos.get(which)

    if isinstance(v, dict):
        s = v.get("symbol") or v.get("ticker") or v.get("name")
        if s:
            return s
        addr = v.get("address") or v.get("token_address") or v.get("tokenAddress")
        if addr:
            return ADDRESS_SYMBOL_MAP.get(str(addr).strip().lower(), "TOKEN")

    if isinstance(v, str):
        m = ADDRESS_SYMBOL_MAP.get(v.strip().lower())
        if m:
            return m

    toks = pos.get("tokens")
    if isinstance(toks, list) and len(toks) >= 2:
        idx = 0 if which == "token0" else 1
        t = toks[idx]
        if isinstance(t, dict):
            s = t.get("symbol") or t.get("ticker") or t.get("name")
            if s:
                return s
            addr = t.get("address") or t.get("token_address") or t.get("tokenAddress")
            if addr:
                m = ADDRESS_SYMBOL_MAP.get(str(addr).strip().lower())
                if m:
                    return m

    return "TOKEN"


# ----------------
# Net USD
# ----------------
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
        return max(float(latest), 0.0)

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


# ----------------
# 24h fee calc
# ----------------
def calc_fee_usd_24h_from_cash_flows(pos_list_all, now_dt):
    end_dt = now_dt.replace(hour=9, minute=0, second=0, microsecond=0)
    if now_dt < end_dt:
        end_dt -= timedelta(days=1)
    start_dt = end_dt - timedelta(days=1)

    total = 0.0
    total_count = 0
    fee_by_nft = {}
    count_by_nft = {}

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

            # 現仕様：fees/collect/claim を含むものを対象（維持）
            if not any(k in t for k in ("fee", "collect", "claim")):
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

            if not (amt_usd > 0):
                continue

            total += amt_usd
            total_count += 1
            fee_by_nft[nft_id] = fee_by_nft.get(nft_id, 0.0) + amt_usd
            count_by_nft[nft_id] = count_by_nft.get(nft_id, 0) + 1

    return total, total_count, fee_by_nft, count_by_nft, start_dt, end_dt


# ----------------
# config
# ----------------
def load_config():
    path = os.environ.get("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ----------------
# report builder
# ----------------
def build_daily_report_for_safe(safe: str):
    positions_open = fetch_positions(safe, active=True)
    positions_exited = fetch_positions(safe, active=False)

    pos_list_open = (
        positions_open
        if isinstance(positions_open, list)
        else positions_open.get("positions", positions_open.get("data", []))
    )
    pos_list_exited = (
        positions_exited
        if isinstance(positions_exited, list)
        else positions_exited.get("positions", positions_exited.get("data", []))
    )

    pos_list_all = []
    if isinstance(pos_list_open, list):
        pos_list_all += pos_list_open
    if isinstance(pos_list_exited, list):
        pos_list_all += pos_list_exited

    now_dt = datetime.now(JST)
    fee_usd, fee_count, fee_by_nft, _, _, end_dt = calc_fee_usd_24h_from_cash_flows(pos_list_all, now_dt)

    nft_lines = []
    net_total = 0.0
    unclaimed_total = 0.0  # fees_value合算(USD)

    for pos in (pos_list_open if isinstance(pos_list_open, list) else []):
        nft_id = str(pos.get("nft_id", "UNKNOWN"))

        in_range = pos.get("in_range")
        status = "OUT OF RANGE" if in_range is False else "ACTIVE"

        net = float(calc_net_usd(pos) or 0.0)
        net_total += net

        fees_value = float(to_f(pos.get("fees_value"), 0.0) or 0.0)
        unclaimed_total += fees_value

        fee_usd_nft = float(fee_by_nft.get(nft_id, 0.0) or 0.0)

        # NEW APR（NFT）: (24h確定 + 未Claim) / Net * 365
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

    # SAFE APR（NEW）: (24h確定 + 未Claim合計) / Net合算 * 365
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


# ----------------
# main
# ----------------
def main():
    mode = get_report_mode()
    print(f"DBG REPORT_MODE={mode}")
    cfg = load_config()
    safes = cfg.get("safes") or []
    if not safes:
        print("config.json: safes is empty", flush=True)
        return

    for s in safes:
        dbg("SAFE CFG:", s)
        name = s.get("name") or "NONAME"
        safe = s.get("safe_address")
        chat_id = s.get("telegram_chat_id")
        
        if not safe or not chat_id:
            print(f"skip: missing safe/chat_id name={name}", flush=True)
            continue

    try:
        if mode == "WEEKLY":
            report = build_weekly_report_for_safe(safe)
# ==========================
# Weekly
# ==========================
def build_weekly_report_for_safe(safe: str) -> str:
    """
    Weekly report (SAFE単位)
    - 7日確定手数料（cash_flows: fees-collected / claimed-fees の合計）
    - Transactions(7d)
    - 前週比：確定収益（7d）だけ（Netは出さない方針）
    - All-time：確定手数料累計（cash_flows全期間の合計）
    """
    # TODO: ここは次に実装する（まずは動作確認用）
    end_dt = get_period_end_jst()  # Dailyと同じ「今日の09:00 JST」
    start_dt = end_dt - timedelta(days=7)

    report = (
        "CBC Liquidity Mining — Weekly\n"
        f"Week Ending: {end_dt.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"Period: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')} JST\n"
        "────────────────\n"
        f"SAFE\n{safe}\n\n"
        "・7日確定手数料 $0.00\n"
        "・Weekly APR（確定基準） 0.00%\n"
        "・Transactions（7d） 0\n"
        "・前週比（確定収益） N/A\n"
        "・先週との差額（確定） N/A\n"
        "・累計確定（All-time） N/A\n"
    )
    return report


# ==========================
# main
# ==========================
def main():
    mode = get_report_mode()
    print(f"DBG REPORT_MODE={mode}")

    cfg = load_config()
    safes = cfg.get("safes") or []
    if not safes:
        print("config.json: safes is empty", flush=True)
        return

    for s in safes:
        dbg("SAFE CFG:", s)
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
            try:
                send_telegram(f"CBC LM ERROR\nNAME: {name}\nSAFE: {safe}\n\n{e}", chat_id)
            except Exception:
                pass


if __name__ == "__main__":
    main()
