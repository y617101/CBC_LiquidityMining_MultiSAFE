import os
import json
import html
import time
import requests
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials


# ================================
# Constants
# ================================
JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"

# Token Symbol Map (Base)
ADDRESS_SYMBOL_MAP = {
    "0x4200000000000000000000000000000000000006": "WETH",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
}


# ================================
# Helpers
# ================================
def dbg(*args):
    if (os.getenv("DEBUG") or "").strip() == "1":
        print(*args, flush=True)


def h(x) -> str:
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
        if ts_i > 10_000_000_000:  # ms -> sec
            ts_i //= 1000
        return ts_i
    except Exception:
        return None


def env_int(name: str, default: int = 0) -> int:
    """
    ENVをint化（不正値でも落とさず default にフォールバック）
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    s = str(raw).strip()
    if s == "":
        return default
    try:
        return int(s)
    except Exception:
        print(f"DBG ENV int parse failed: {name} raw={raw!r} -> default {default}", flush=True)
        return default


# ================================
# Mode / Time
# ================================
def get_report_mode() -> str:
    return (os.getenv("REPORT_MODE") or "DAILY").strip().upper()


def get_period_end_jst(now: datetime | None = None) -> datetime:
    """
    Period end aligned to 09:00 JST (today 09:00 JST).
    """
    if now is None:
        now = datetime.now(JST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=JST)
        else:
            now = now.astimezone(JST)
    return now.replace(hour=9, minute=0, second=0, microsecond=0)


# ================================
# Sheets (429-safe wrapper)
# ================================
def sheets_call(fn, *args, **kwargs):
    """
    Google Sheets APIの429を安全に吸収（指数バックオフ）
    """
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if "APIError: [429]" in msg or "Quota exceeded" in msg:
                wait = 2 ** attempt  # 1,2,4,8,16...
                print(f"DBG: sheets 429 -> retry after {wait}s", flush=True)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Sheets quota 429 retry exhausted")


def get_gsheet():
    creds = Credentials.from_service_account_file(
        "gcp_service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("ENV missing: GOOGLE_SHEET_ID")
    return client.open_by_key(sheet_id)


# ================================
# Telegram
# ================================
def send_telegram(text: str, chat_id: str):
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        print("Telegram ENV missing: TG_BOT_TOKEN", flush=True)
        return
    if not chat_id:
        print("Telegram chat_id missing", flush=True)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

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


# ================================
# APR
# ================================
def calc_fee_apr_a(fee_24h_usd, net_usd):
    """
    APR% = fee_usd / net_usd * 365 * 100
    """
    if fee_24h_usd is None or net_usd is None or net_usd <= 0:
        return None
    return (fee_24h_usd / net_usd) * 365 * 100


# ================================
# 24h fee calc (Daily)
# ================================
def calc_fee_usd_24h_from_cash_flows(pos_list_all, now_dt: datetime):
    """
    24h窓（JST 09:00 → 翌09:00）で「確定手数料USD」を集計
    - type に "fee"/"collect"/"claim" を含むものを対象
    - amount_usd が無ければ prices×数量 でフォールバック
    Returns:
      total_usd, total_count, fee_by_nft, count_by_nft, start_dt, end_dt
    """
    end_dt = get_period_end_jst(now_dt)
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
                DEBUG_FEE_TRACE = (os.getenv("DEBUG_FEE_TRACE") or "").strip() == "1"

            t = _lower(cf.get("type"))
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

            if DEBUG_FEE_TRACE:
                print(
                    "DBG_FEE",
                    "nft=", nft_id,
                    "type=", t,
                    "ts_jst=", ts_dt.strftime("%Y-%m-%d %H:%M"),
                    "amount_usd_raw=", cf.get("amount_usd"),
                    "final_usd=", amt_usd,
                    flush=True
                )

            total += amt_usd
            total_count += 1

            fee_by_nft[nft_id] = float(fee_by_nft.get(nft_id, 0.0) or 0.0) + amt_usd
            count_by_nft[nft_id] = int(count_by_nft.get(nft_id, 0) or 0) + 1

    return total, total_count, fee_by_nft, count_by_nft, start_dt, end_dt


# ================================
# Weekly fee calc
# ================================
def calc_fees_usd_in_window_from_cash_flows(pos_list_all, start_dt: datetime, end_dt: datetime):
    """
    Sum fees (USD) in [start_dt, end_dt) from positions cash_flows.
    Target types: fees-collected / claimed-fees
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
# Daily report
# ================================
def build_daily_report_for_safe(safe: str, end_dt=None, block_level: str = "FULL"):
    """
    block_level:
      - "FULL"    : NFTブロック詳細
      - "COMPACT" : 短く
      - "NONE"    : NFTブロック無し（SAFE集計だけ）
    """
    if end_dt is None:
        end_dt = get_period_end_jst()

    start_dt = end_dt - timedelta(days=1)

    positions_open = fetch_positions(safe, active=True)
    positions_exited = fetch_positions(safe, active=False)

    pos_list_open = _normalize_positions(positions_open)
    pos_list_exited = _normalize_positions(positions_exited)

    pos_list_all = []
    pos_list_all += pos_list_open
    pos_list_all += pos_list_exited

    now_dt = end_dt

    fee_usd, fee_count, fee_by_nft, _, start_dt, end_dt = calc_fee_usd_24h_from_cash_flows(
        pos_list_all, now_dt
    )

    nft_blocks = []
    net_total = 0.0
    unclaimed_total = 0.0  # fees_value合算(USD)

    lvl = (block_level or "FULL").upper()

    for pos in pos_list_open:
        nft_id = str(pos.get("nft_id", "UNKNOWN"))

        in_range = pos.get("in_range")
        status = "OUT OF RANGE" if in_range is False else "ACTIVE"

        net = float(calc_net_usd(pos) or 0.0)
        net_total += net

        fees_value = float(to_f(pos.get("fees_value"), 0.0) or 0.0)
        unclaimed_total += fees_value

        fee_usd_nft = float(fee_by_nft.get(nft_id, 0.0) or 0.0)

        # NFT APR = (24h確定 + 未Claim) / Net * 365
        nft_apr_base = fee_usd_nft + fees_value
        nft_fee_apr = calc_fee_apr_a(nft_apr_base, net)

        if lvl == "NONE":
            continue

        nft_url = f"https://app.uniswap.org/positions/v3/base/{nft_id}"
        nft_link = f'<a href="{h(nft_url)}">{h(nft_id)}</a>'

        if lvl == "COMPACT":
            nft_blocks.append(
                "\n"
                f"NFT {nft_link} / {h(status)}\n"
                f"Net {fmt_money(net)} | 未Claim {fmt_money(fees_value)} | APR {fmt_pct(nft_fee_apr)}\n"
            )
        else:
            nft_blocks.append(
                "\n"
                "———————\n"
                f"NFT {nft_link}\n"
                f"Status {h(status)}\n"
                "———————\n"
                "Net\n"
                f"{fmt_money(net)}\n\n"
                "蓄積手数料（未Claim）\n"
                f"{fmt_money(fees_value)}\n\n"
                "Fee APR\n"
                f"{fmt_pct(nft_fee_apr)}\n"
            )

    # SAFE APR = (24h確定 + 未Claim合計) / Net合算 * 365
    apr_base_usd = float(fee_usd or 0.0) + float(unclaimed_total or 0.0)
    safe_fee_apr = calc_fee_apr_a(apr_base_usd, net_total)

    report = (
        "CBC Liquidity Mining — Daily\n"
        "\n"
        "SAFE\n"
        f"{safe}\n"
        "\n"
        "Net合算\n"
        f"{fmt_money(net_total)}\n"
        "\n"
        "———————\n"
        "推定総収益（24h＋未Claim）\n"
        f"{fmt_money(apr_base_usd)}\n"
        "———————\n"
        "\n"
        "確定手数料（24h）\n"
        f"{fmt_money(fee_usd)}\n"
        "\n"
        "蓄積手数料（未Claim）\n"
        f"{fmt_money(unclaimed_total)}\n"
        "\n"
        "Fee APR (SAFE)\n"
        f"{fmt_pct(safe_fee_apr)}\n"
        "\n"
        "Transactions\n"
        f"{fee_count}\n"
        + "".join(nft_blocks)
        + "\n"
        "Period\n"
        f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}\n"
        "09:00 JST\n"
    )

    return report, float(fee_usd or 0.0), end_dt


# ================================
# Weekly report
# ================================
def build_weekly_report_for_safe(safe: str) -> str:
    end_dt = get_period_end_jst()
    start_dt = end_dt - timedelta(days=7)

    positions_open = fetch_positions(safe, active=True)
    positions_exited = fetch_positions(safe, active=False)

    pos_list_open = _normalize_positions(positions_open)
    pos_list_exited = _normalize_positions(positions_exited)

    pos_list_all = []
    pos_list_all += pos_list_open
    pos_list_all += pos_list_exited

    fee_7d_usd, tx_7d = calc_fees_usd_in_window_from_cash_flows(pos_list_all, start_dt, end_dt)
    fee_all_time_usd, _ = calc_fees_usd_in_window_from_cash_flows(
        pos_list_all, datetime(2000, 1, 1, tzinfo=JST), end_dt
    )

    net_total = 0.0
    for pos in pos_list_open:
        net_total += float(calc_net_usd(pos) or 0.0)

    avg_daily = fee_7d_usd / 7.0
    weekly_apr_pct = (fee_7d_usd / net_total) * 52 * 100 if net_total > 0 else 0.0

    report = (
        "CBC Liquidity Mining — Weekly\n"
        "\n"
        "SAFE\n"
        f"{safe}\n"
        "\n"
        "———————\n"
        "7日確定手数料\n"
        f"{fmt_money(fee_7d_usd)}\n"
        "———————\n"
        "\n"
        "平均確定手数料\n"
        f"{fmt_money(avg_daily)} / day\n"
        "\n"
        "Weekly APR\n"
        f"{fmt_pct(weekly_apr_pct)}\n"
        "\n"
        "Transactions（7d）\n"
        f"{tx_7d}\n"
        "———————\n"
        "累計確定（All-time）\n"
        f"{fmt_money(fee_all_time_usd)}\n"
        "———————\n"
        "\n"
        "Period\n"
        f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}\n"
        "09:00 JST\n"
    )
    return report


# ================================
# Sheets: DAILY_WIDE
# Row1: No, Row2: SAFE name, Row3: safe_address, Row4+: daily values
# ================================
def append_daily_wide_numbered(period_end_jst, safe_name, safe_address, claimed_usd_24h):
    sh = get_gsheet()
    tab_name = os.getenv("GOOGLE_SHEET_DAILY_TAB", "DAILY_WIDE")
    ws = sh.worksheet(tab_name)

    # Sheets表示寄せ（例: 2026-02-22 9:00）
    period_key = f"{period_end_jst.strftime('%Y-%m-%d')} {period_end_jst.hour}:{period_end_jst.strftime('%M')}"

    values = sheets_call(ws.get_all_values) or []

    # --- 初期化（空シート） ---
    if not values:
        sheets_call(ws.update, range_name="A1", values=[["", 1]])
        sheets_call(ws.update, range_name="A2", values=[["period_end_jst", safe_name]])
        sheets_call(ws.update, range_name="A3", values=[["safe_address", safe_address]])
        sheets_call(ws.update, range_name="A4", values=[[period_key, float(claimed_usd_24h)]])
        print("DBG: initialized DAILY_WIDE", flush=True)
        return

    # --- ヘッダー行を確保 ---
    if len(values) < 2:
        sheets_call(ws.update, range_name="A2", values=[["period_end_jst"]])
        values = sheets_call(ws.get_all_values) or []
    if len(values) < 3:
        sheets_call(ws.update, range_name="A3", values=[["safe_address"]])
        values = sheets_call(ws.get_all_values) or []

    header_nums = values[0] if len(values) >= 1 else [""]
    header_names = values[1] if len(values) >= 2 else ["period_end_jst"]
    header_addrs = values[2] if len(values) >= 3 else ["safe_address"]

    if not header_names or header_names[0] != "period_end_jst":
        if header_names:
            header_names[0] = "period_end_jst"
        else:
            header_names = ["period_end_jst"]

    if not header_addrs or header_addrs[0] != "safe_address":
        if header_addrs:
            header_addrs[0] = "safe_address"
        else:
            header_addrs = ["safe_address"]

    # --- SAFE列が無ければ追加 ---
    if safe_name not in header_names:
        current_safe_cols = max(0, len(header_names) - 1)  # A列除く
        next_no = current_safe_cols + 1

        max_len = max(len(header_nums), len(header_names), len(header_addrs))
        header_nums += [""] * (max_len - len(header_nums))
        header_names += [""] * (max_len - len(header_names))
        header_addrs += [""] * (max_len - len(header_addrs))

        header_nums.append(next_no)
        header_names.append(safe_name)
        header_addrs.append(safe_address)

        sheets_call(ws.update, range_name="A1", values=[header_nums])
        sheets_call(ws.update, range_name="A2", values=[header_names])
        sheets_call(ws.update, range_name="A3", values=[header_addrs])

        print("DBG: added SAFE column", safe_name, "no", next_no, flush=True)
    else:
        col_idx = header_names.index(safe_name) + 1  # 1-based
        existing = header_addrs[col_idx - 1] if len(header_addrs) >= col_idx else ""
        if not str(existing or "").strip():
            sheets_call(ws.update_cell, 3, col_idx, safe_address)

    # --- ここから値を書き込む ---
    values = sheets_call(ws.get_all_values) or []
    header_names = values[1] if len(values) >= 2 else ["period_end_jst"]
    col_idx = header_names.index(safe_name) + 1  # 1-based

    row_idx = None
    for i, row in enumerate(values[3:], start=4):
        if len(row) >= 1 and row[0].strip() == period_key:
            row_idx = i
            break

    if row_idx is None:
        sheets_call(ws.append_row, [period_key], value_input_option="USER_ENTERED")
        row_idx = len(values) + 1

    sheets_call(ws.update_cell, row_idx, col_idx, float(claimed_usd_24h))
    print("DBG: DAILY_WIDE updated", period_key, safe_name, claimed_usd_24h, flush=True)


def maybe_sort_daily_wide_by_date():
    """
    任意：A列で昇順ソート（見た目を時系列に整える）
    ENV: SHEETS_SORT_BY_DATE=1
    """
    if (os.getenv("SHEETS_SORT_BY_DATE") or "").strip() != "1":
        return

    sh = get_gsheet()
    tab_name = os.getenv("GOOGLE_SHEET_DAILY_TAB", "DAILY_WIDE")
    ws = sh.worksheet(tab_name)

    values = sheets_call(ws.get_all_values) or []
    if len(values) <= 4:
        return  # データ少なすぎ

    last_row = len(values)
    last_col = max((len(r) for r in values), default=1)

    # A4 から最終行まで、A列で昇順
    try:
        sheets_call(
            ws.sort,
            (1, "asc"),
            range=f"A4:{gspread.utils.rowcol_to_a1(last_row, last_col)}",
        )
        print("DBG: sorted DAILY_WIDE by date (A col asc)", flush=True)
    except Exception as e:
        print(f"DBG: sort skipped err={e}", flush=True)


def main():
    mode = get_report_mode()
    print(f"DBG REPORT_MODE={mode}", flush=True)

    cfg = load_config()
    safes = cfg.get("safes") or []
    if not safes:
        print("config.json: safes is empty", flush=True)
        return

    # Backfill flags
    backfill_once_raw = os.getenv("BACKFILL_ONCE")
    backfill_days_raw = os.getenv("BACKFILL_DAYS")

    backfill_once = (backfill_once_raw or "").strip() == "1"
    backfill_days = env_int("BACKFILL_DAYS", 0)
    backfill_offset = env_int("BACKFILL_OFFSET_DAYS", 0)

    print(f"DBG BACKFILL_ONCE raw={backfill_once_raw!r} parsed={backfill_once}", flush=True)
    print(f"DBG BACKFILL_DAYS raw={backfill_days_raw!r} parsed={backfill_days}", flush=True)
    print(f"DBG BACKFILL_OFFSET_DAYS raw={os.getenv('BACKFILL_OFFSET_DAYS')!r} parsed={backfill_offset}", flush=True)

    only_name_raw = os.getenv("BACKFILL_ONLY_NAME")
    only_name = (only_name_raw or "").strip().upper()

    if only_name:
        print(f"DBG BACKFILL_ONLY_NAME raw={only_name_raw!r} parsed={only_name!r}", flush=True)

    for s in safes:
        name = (s.get("name") or "NONAME").strip()
        name_upper = name.upper()

        safe = s.get("safe_address")
        chat_id = s.get("telegram_chat_id")

        if only_name:
            if name_upper != only_name:
                print(f"DBG: skip by BACKFILL_ONLY_NAME name={name!r}", flush=True)
                continue

        if not safe:
            print(f"skip: missing safe name={name}", flush=True)
            continue

        if (not backfill_once or backfill_days <= 0) and not chat_id:
            print(f"skip: missing chat_id name={name}", flush=True)
            continue

        try:
            if mode == "WEEKLY":
                report = build_weekly_report_for_safe(safe)
                send_telegram(report, chat_id)

            else:
                if backfill_once and backfill_days > 0:
                    print(f"DBG: start backfill name={name}", flush=True)

                    for d in range(backfill_days, 0, -1):
                        # ★ offset 対応（ここが今回の肝）
                        bf_end_dt = get_period_end_jst() - timedelta(days=backfill_offset + d)
                        bf_key = bf_end_dt.strftime("%Y-%m-%d %H:%M")

                        try:
                            _report, fee_usd, _ = build_daily_report_for_safe(
                                safe, bf_end_dt, block_level="NONE"
                            )
                            append_daily_wide_numbered(bf_end_dt, name, safe, fee_usd)

                            print(f"DBG: backfill ok {name} {bf_key} {fee_usd}", flush=True)
                            time.sleep(0.6)

                        except Exception as day_e:
                            msg = str(day_e)
                            if "APIError: [429]" in msg or "Quota exceeded" in msg:
                                print(f"DBG: backfill stop by 429 {name} {bf_key}", flush=True)
                                break
                            print(f"DBG: backfill skip {name} {bf_key} err={day_e}", flush=True)
                            continue

                    print(f"DBG: backfill done name={name}", flush=True)

                    # 任意：並びを整える（必要なら ENV でON）
                    maybe_sort_daily_wide_by_date()

                else:
                    report, fee_usd, end_dt = build_daily_report_for_safe(safe)
                    send_telegram(report, chat_id)
                    append_daily_wide_numbered(end_dt, name, safe, fee_usd)

        except Exception as e:
            print(f"error name={name} safe={safe}: {e}", flush=True)
            if chat_id:
                try:
                    send_telegram(
                        f"CBC LM ERROR\n\nNAME\n{name}\n\nSAFE\n{safe}\n\nERROR\n{e}",
                        chat_id,
                    )
                except Exception:
                    pass


if __name__ == "__main__":
    main()
