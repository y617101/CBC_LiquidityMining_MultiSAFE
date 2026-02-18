import os
import json
import requests
from datetime import datetime, timedelta, timezone

# ================================
# Token Symbol Map (Base)
# ================================
ADDRESS_SYMBOL_MAP = {
    "0xB1A76A21769a90Ce4fE0edC3DBBae0b9E7689734": "WETH",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
}

JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"


def send_telegram(text: str, chat_id: str):
    token = os.environ.get("TG_BOT_TOKEN")
    if not token:
        print("Telegram ENV missing: TG_BOT_TOKEN", flush=True)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Telegramは4096文字制限 → 3900で分割
    s = str(text)
    chunks = [s[i:i+3900] for i in range(0, len(s), 3900)]

    for i, chunk in enumerate(chunks, 1):
        r = requests.post(
            url,
            json={"chat_id": chat_id, "text": chunk},
            timeout=30
        )
        print(f"Telegram part {i}/{len(chunks)} status:", r.status_code, flush=True)
        r.raise_for_status()



def fetch_positions(safe: str, active: bool = True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def to_f(x, default=None):
    try:
        return float(x)
    except:
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
    except:
        return None


def calc_fee_apr_a(fee_24h_usd, net_usd):
    if fee_24h_usd is None or net_usd is None or net_usd <= 0:
        return None
    return (fee_24h_usd / net_usd) * 365 * 100


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

    # 2) total_debt が無い場合のみ、borrows - repays をUSDで集計（USDフィールド優先）
    borrow_usd = 0.0
    repay_usd = 0.0

    for cf in cfs:
        if not isinstance(cf, dict):
            continue
        t = _lower(cf.get("type"))
        if t not in ("lendor-borrow", "lendor-repay"):
            continue

        v = to_f(cf.get("amount_usd"))
        if v is None: v = to_f(cf.get("usd"))
        if v is None: v = to_f(cf.get("value_usd"))
        if v is None: v = to_f(cf.get("valueUsd"))
        if v is None: v = to_f(cf.get("amountUsd"))

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

            # あなたの現仕様：fees/collect/claim を含むものを対象（ここは現状維持）
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

                q0 = to_f(cf.get("collected_fees_token0")) or to_f(cf.get("claimed_token0")) or to_f(cf.get("fees0")) or to_f(cf.get("amount0")) or 0.0
                q1 = to_f(cf.get("collected_fees_token1")) or to_f(cf.get("claimed_token1")) or to_f(cf.get("fees1")) or to_f(cf.get("amount1")) or 0.0

                amt_usd = abs(q0) * p0 + abs(q1) * p1

            try:
                amt_usd = float(amt_usd)
            except:
                continue
            if not (amt_usd > 0):
                continue

            total += amt_usd
            total_count += 1
            fee_by_nft[nft_id] = fee_by_nft.get(nft_id, 0.0) + amt_usd
            count_by_nft[nft_id] = count_by_nft.get(nft_id, 0) + 1

    return total, total_count, fee_by_nft, count_by_nft, start_dt, end_dt


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


def load_config():
    path = os.environ.get("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_daily_report_for_safe(safe: str):
    positions_open = fetch_positions(safe, active=True)
    positions_exited = fetch_positions(safe, active=False)

    pos_list_open = positions_open if isinstance(positions_open, list) else positions_open.get("positions", positions_open.get("data", []))
    pos_list_exited = positions_exited if isinstance(positions_exited, list) else positions_exited.get("positions", positions_exited.get("data", []))

    pos_list_all = []
    if isinstance(pos_list_open, list):
        pos_list_all += pos_list_open
    if isinstance(pos_list_exited, list):
        pos_list_all += pos_list_exited

    now_dt = datetime.now(JST)
    fee_usd, fee_count, fee_by_nft, count_by_nft, start_dt, end_dt = calc_fee_usd_24h_from_cash_flows(pos_list_all, now_dt)

    nft_lines = []
    net_total = 0.0
    uncollected_total = 0.0

    for pos in (pos_list_open if isinstance(pos_list_open, list) else []):
        nft_id = str(pos.get("nft_id", "UNKNOWN"))

        in_range = pos.get("in_range")
        status = "ACTIVE" if in_range is not False else "OUT OF RANGE"

        net = calc_net_usd(pos)
        if net is not None:
            net_total += float(net)

        fees_value = to_f(pos.get("fees_value"), 0.0)
        uncollected_total += fees_value

        u0 = pos.get("uncollected_fees0")
        u1 = pos.get("uncollected_fees1")
        sym0 = resolve_symbol(pos, "token0")
        sym1 = resolve_symbol(pos, "token1")

        # ✅ 方式Aを表示（いまのコードはperformanceのfee_aprを表示してたので、ここで統一）
        fee_usd_nft = fee_by_nft.get(str(nft_id), 0.0)
        fee_apr_nft = calc_fee_apr_a(fee_usd_nft, net)

        nft_lines.append(
            f"\nNFT {nft_id}\n"
            f"Status: {status}\n"
            f"Net: {fmt_money(net)}\n"
            f"Uncollected: {fees_value:.2f} USD\n"
            f"Uncollected Fees:\n"
            f"{to_f(u0, 0.0):.8f} {sym0}\n"
            f"{to_f(u1, 0.0):.6f} {sym1}\n"
            f"Fee APR: {fmt_pct(fee_apr_nft)}\n"
        )

    safe_fee_apr = calc_fee_apr_a(fee_usd, net_total)

    report = (
        "CBC Liquidity Mining — Daily\n"
        f"Period End: {end_dt.strftime('%Y-%m-%d %H:%M')} JST\n"
        "────────────────\n"
        f"SAFE\n{safe}\n\n"
        f"・24h確定手数料 {fmt_money(fee_usd)}\n"
        f"・Fee APR(SAFE) {fmt_pct(safe_fee_apr)}\n"
        f"・Net合算 {fmt_money(net_total)}\n"
        f"・未回収手数料 {fmt_money(uncollected_total)}\n"
        f"・Transactions {fee_count}\n"
        f"・Period {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')} JST\n"
        + "".join(nft_lines)
    )

    return report


def main():
    cfg = load_config()
    safes = cfg.get("safes") or []
    if not safes:
        print("config.json: safes is empty", flush=True)
        return

    for s in safes:
        name = s.get("name") or "NONAME"
        safe = s.get("safe_address")
        chat_id = s.get("telegram_chat_id")

        if not safe or not chat_id:
            print(f"skip: missing safe/chat_id name={name}", flush=True)
            continue

        try:
            report_body = build_daily_report_for_safe(safe)

            header = (
                "CBC Liquidity Mining — Daily (MULTI)\n"
                "────────────────\n"
                f"NAME: {name}\n"
                f"SAFE: {safe}\n"
                "STATUS: OK\n\n"
            )

            full_report = header + report_body

            send_telegram(full_report, chat_id)

        except Exception as e:
            print(f"error name={name} safe={safe}: {e}", flush=True)
            try:
                send_telegram(
                    f"CBC LM ERROR\nNAME: {name}\nSAFE: {safe}\n{e}",
                    chat_id
                )
            except:
                pass



if __name__ == "__main__":
    main()
