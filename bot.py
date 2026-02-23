import os
import json
import html
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# ================================
# Constants
# ================================
JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"

# ================================
# Small helpers
# ================================
def build_basescan_addr_link(addr: str) -> str:
    a = (addr or "").strip()
    return f"https://basescan.org/address/{a}"

def mask_safe_addr(addr: str) -> str:
    a = (addr or "").strip()
    # 0x + 先頭5 + 末尾4 を満たさない短いアドレスはそのまま
    if len(a) <= (2 + 5 + 4):
        return a
    return f"{a[:2+5]}*****{a[-4:]}"  # 例: 0xB1A76*****89734

def fmt_safe_link(addr: str) -> str:
    url = build_basescan_addr_link(addr)
    label = mask_safe_addr(addr)
    return f'<a href="{h(url)}">{h(label)}</a>'
    
def build_uniswap_link_base(nft_id: str) -> str:
    return f"https://app.uniswap.org/positions/v3/base/{nft_id}"

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

def _get_tx_hash(cf: dict) -> str:
    return str(
        cf.get("tx_hash")
        or cf.get("txHash")
        or cf.get("transaction_hash")
        or cf.get("transactionHash")
        or ""
    ).strip()

def dbg(*args):
    if (os.getenv("DEBUG") or "").strip() == "1":
        print(*args, flush=True)

# ================================
# Time (JST 09:00 anchor)
# ================================
def get_period_end_jst(now: Optional[datetime] = None) -> datetime:
    if now is None:
        now = datetime.now(JST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=JST)
        else:
            now = now.astimezone(JST)
    return now.replace(hour=9, minute=0, second=0, microsecond=0)

def pick_mode_auto(now: Optional[datetime] = None) -> str:
    """
    Sunday only WEEKLY, otherwise DAILY.
    Python weekday: Mon=0 ... Sun=6
    """
    now = now or datetime.now(JST)
    return "WEEKLY" if now.weekday() == 6 else "DAILY"

def get_mode() -> str:
    """
    If REPORT_MODE is set explicitly -> honor it.
    Else -> AUTO by weekday.
    """
    raw = (os.getenv("REPORT_MODE") or "").strip().upper()
    if raw in ("DAILY", "WEEKLY"):
        return raw
    return pick_mode_auto()

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
# Sheets (429-safe)
# ================================
def sheets_call(fn, *args, **kwargs):
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if "APIError: [429]" in msg or "Quota exceeded" in msg:
                wait = 2 ** attempt
                print(f"DBG: sheets 429 -> retry after {wait}s", flush=True)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Sheets quota 429 retry exhausted")

def get_gsheet_client():
    creds = Credentials.from_service_account_file(
        "gcp_service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def open_sheet(client):
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("ENV missing: GOOGLE_SHEET_ID")
    return client.open_by_key(sheet_id)

# ---- DAILY_LOG (縦) ----
def get_log_ws(sh):
    tab_name = os.getenv("GOOGLE_SHEET_LOG_TAB", "DAILY_LOG")
    return sh.worksheet(tab_name)

def ensure_log_header(ws):
    values = sheets_call(ws.get_all_values) or []
    if values and values[0] and values[0][0].strip().lower() == "period_end_jst":
        return
    header = [
        "period_end_jst",
        "safe_name",
        "safe_address",
        "net_total_usd",
        "claimed_24h_usd",
        "unclaimed_usd",
        "emitted_usd",
    ]
    sheets_call(ws.update, range_name="A1", values=[header])
    print("DBG: initialized DAILY_LOG header", flush=True)

def _parse_period_key(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=JST)
    except Exception:
        return None

def read_log_rows(ws) -> List[List[str]]:
    values = sheets_call(ws.get_all_values) or []
    if len(values) <= 1:
        return []
    return values[1:]

def upsert_daily_log_row(
    ws,
    period_end: datetime,
    safe_name: str,
    safe_address: str,
    net_total: float,
    claimed_24h: float,
    unclaimed: float,
    emitted: float,
):
    period_key = period_end.strftime("%Y-%m-%d %H:%M")
    rows = read_log_rows(ws)

    target_row_idx = None
    for i, row in enumerate(rows, start=2):  # header row=1
        if len(row) < 3:
            continue
        if row[0].strip() == period_key and row[2].strip().lower() == safe_address.strip().lower():
            target_row_idx = i
            break

    record = [
        period_key,
        safe_name,
        safe_address,
        f"{float(net_total):.6f}",
        f"{float(claimed_24h):.6f}",
        f"{float(unclaimed):.6f}",
        f"{float(emitted):.6f}",
    ]

    if target_row_idx is None:
        sheets_call(ws.append_row, record, value_input_option="USER_ENTERED")
        dbg("DBG: appended log row", period_key, safe_name)
    else:
        rng = f"A{target_row_idx}:G{target_row_idx}"
        sheets_call(ws.update, range_name=rng, values=[record])
        dbg("DBG: updated log row", period_key, safe_name)

# ---- DAILY_WIDE (横) ----
def get_daily_wide_ws(sh):
    tab_name = os.getenv("GOOGLE_SHEET_DAILY_WIDE_TAB", "DAILY_WIDE")
    return sh.worksheet(tab_name)

def append_daily_wide_numbered(ws, period_end_jst, safe_name, safe_address, claimed_usd_24h):
    # スクショ寄せ: 2026-02-23 9:00
    period_key = f"{period_end_jst.strftime('%Y-%m-%d')} {period_end_jst.hour}:{period_end_jst.strftime('%M')}"

    values = sheets_call(ws.get_all_values) or []

    # 初期化（空）
    if not values:
        sheets_call(ws.update, range_name="A1", values=[["", 1]])
        sheets_call(ws.update, range_name="A2", values=[["period_end_jst", safe_name]])
        sheets_call(ws.update, range_name="A3", values=[["safe_address", safe_address]])
        sheets_call(ws.update, range_name="A4", values=[[period_key, float(claimed_usd_24h)]])
        print("DBG: initialized DAILY_WIDE", flush=True)
        return

    # ヘッダー確保
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
        header_names = ["period_end_jst"] + header_names[1:]
    if not header_addrs or header_addrs[0] != "safe_address":
        header_addrs = ["safe_address"] + header_addrs[1:]

    # SAFE列追加
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

    col_idx = header_names.index(safe_name) + 1  # 1-based

    # 日付行検索（4行目以降）
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

# ================================
# Revert API
# ================================
def fetch_positions(safe: str, active: bool = True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _normalize_positions(resp) -> list:
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
# Net USD (pooled - debt)
# ================================
def extract_debt_usd(pos) -> float:
    cfs = pos.get("cash_flows") or []
    if not isinstance(cfs, list):
        return 0.0

    # Prefer latest total_debt from lendor flows
    latest_td = None
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
            latest_td = td

    if latest_td is not None:
        return max(float(latest_td), 0.0)

    # Fallback: borrows - repays by USD
    borrow = 0.0
    repay = 0.0
    for cf in cfs:
        if not isinstance(cf, dict):
            continue
        t = _lower(cf.get("type"))
        if t not in ("lendor-borrow", "lendor-repay"):
            continue
        v = (
            to_f(cf.get("amount_usd"))
            or to_f(cf.get("usd"))
            or to_f(cf.get("value_usd"))
            or to_f(cf.get("valueUsd"))
            or to_f(cf.get("amountUsd"))
        )
        if v is None:
            continue
        v = abs(float(v))
        if t == "lendor-borrow":
            borrow += v
        else:
            repay += v

    debt = borrow - repay
    return debt if debt > 0 else 0.0

def calc_net_usd(pos) -> Optional[float]:
    pooled = to_f(pos.get("underlying_value"))
    if pooled is None:
        return None
    debt = extract_debt_usd(pos)
    return float(pooled) - float(debt)

# ================================
# Fees (USD) helpers
# ================================
def _get_cf_usd_ui_first(cf: dict) -> Optional[float]:
    candidates = [
        "hodl_value",
        "hodl_value_usd",
        "hodlValue",
        "hodlValueUsd",
        "hodl_usd",
        "hodlUsd",
        "hodl_valueUsd",
        "hodl_valueUSD",
    ]
    for k in candidates:
        v = to_f(cf.get(k))
        if v is not None:
            return float(v)
    v = to_f(cf.get("amount_usd"))
    if v is not None:
        return float(v)
    return None

def calc_claimed_usd_in_window(pos_list_all: List[dict], start_dt: datetime, end_dt: datetime) -> Tuple[float, int]:
    total = 0.0
    count = 0
    seen = set()  # (tx_hash, type)

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

            txh = _get_tx_hash(cf)
            key = (txh, t)
            if txh and key in seen:
                continue
            if txh:
                seen.add(key)

            amt_usd = _get_cf_usd_ui_first(cf)
            if amt_usd is None:
                continue
            try:
                amt_usd = float(amt_usd)
            except Exception:
                continue
            if amt_usd <= 0:
                continue

            total += amt_usd
            count += 1

    return float(total), int(count)

def calc_claimed_usd_by_nft_in_window(pos_list_all: List[dict], start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    """
    24h確定(=fees-collected/claimed-fees)をNFT別にUSD集計
    open+exited二重計上を避けるため (tx_hash,type,nft_id) で重複排除
    """
    out: Dict[str, float] = {}
    seen = set()  # (txh, type, nft_id)

    for pos in (pos_list_all or []):
        if not isinstance(pos, dict):
            continue
        nft_id = str(pos.get("nft_id") or "UNKNOWN")
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

            txh = _get_tx_hash(cf)
            key = (txh, t, nft_id)
            if txh and key in seen:
                continue
            if txh:
                seen.add(key)

            amt_usd = _get_cf_usd_ui_first(cf)
            if amt_usd is None:
                continue
            try:
                amt_usd = float(amt_usd)
            except Exception:
                continue
            if amt_usd <= 0:
                continue

            out[nft_id] = float(out.get(nft_id, 0.0) or 0.0) + amt_usd

    return out

# ================================
# Metrics derived from Sheets log
# ================================
def _row_safe_addr(row: List[str]) -> str:
    return (row[2] if len(row) > 2 else "").strip().lower()

def _row_period_dt(row: List[str]) -> Optional[datetime]:
    return _parse_period_key(row[0] if row else "")

def _row_val(row: List[str], idx: int) -> float:
    if len(row) <= idx:
        return 0.0
    v = to_f(row[idx])
    return float(v) if v is not None else 0.0

def get_safe_history(rows: List[List[str]], safe_address: str) -> List[List[str]]:
    sa = safe_address.strip().lower()
    out = [r for r in rows if _row_safe_addr(r) == sa]
    out.sort(key=lambda r: (_row_period_dt(r) or datetime(1970, 1, 1, tzinfo=JST)))
    return out

def sum_last_n_days(history: List[List[str]], n: int, value_col_idx: int) -> float:
    if n <= 0:
        return 0.0
    tail = history[-n:] if len(history) >= n else history[:]
    return sum(_row_val(r, value_col_idx) for r in tail)

def sum_prev_n_days(history: List[List[str]], n: int, value_col_idx: int) -> float:
    if n <= 0 or len(history) <= n:
        return 0.0
    prev = history[-2*n:-n] if len(history) >= 2*n else history[:-n]
    return sum(_row_val(r, value_col_idx) for r in prev)

def sum_month_to_date(history: List[List[str]], period_end: datetime, value_col_idx: int) -> float:
    m = period_end.astimezone(JST).month
    y = period_end.astimezone(JST).year
    total = 0.0
    for r in history:
        dt = _row_period_dt(r)
        if not dt:
            continue
        dt = dt.astimezone(JST)
        if dt.year == y and dt.month == m and dt <= period_end.astimezone(JST):
            total += _row_val(r, value_col_idx)
    return total

def sum_all_time(history: List[List[str]], value_col_idx: int) -> float:
    return sum(_row_val(r, value_col_idx) for r in history)

def first_record_dt(history: List[List[str]]) -> Optional[datetime]:
    for r in history:
        dt = _row_period_dt(r)
        if dt:
            return dt
    return None

# ================================
# NFT block (simple)
# ================================
def build_nft_lines_simple(pos_open: List[dict]) -> List[str]:
    lines = []
    for pos in (pos_open or []):
        nft_id = str(pos.get("nft_id") or "").strip()
        if not nft_id:
            continue

        status = "OUT OF RANGE" if pos.get("in_range") is False else "ACTIVE"
        net = float(calc_net_usd(pos) or 0.0)

        # APRは今の計算に合わせて（必要なら差し替え）
        fees_value = float(to_f(pos.get("fees_value"), 0.0) or 0.0)
        apr = (fees_value / net) * 365 * 100 if net > 0 else 0.0

        url = build_uniswap_link_base(nft_id)
        nft_link = f'<a href="{h(url)}">{h(nft_id)}</a>'  # ★フルID＋青リンク

        lines.append(
            f"{nft_link} | {h(status)} | Net {fmt_money(net)} | APR {fmt_pct(apr)}"
        )
    return lines

# ================================
# Build DAILY / WEEKLY messages (templates fixed)
# ================================
def build_daily_message(
    safe_address: str,
    period_end: datetime,
    net_total: float,
    emitted_today: float,
    history: List[List[str]],
    nft_lines: List[str],
) -> str:
    emitted_7d = sum_last_n_days(history, 7, 6)
    avg_emitted_7d = emitted_7d / 7.0 if emitted_7d > 0 else 0.0
    apr_7d = (avg_emitted_7d / net_total) * 365 * 100 if net_total > 0 else 0.0

    mtd_emitted = sum_month_to_date(history, period_end, 6)
    avg_emitted_day_7d = avg_emitted_7d

    safe_link = fmt_safe_link(safe_address)

    msg = (
        "🚀 CBC Liquidity Mining — Daily\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        safe_link = fmt_safe_link(safe_address)  # 0xB1A76*****89734 の青リンク
        ...
        f"SAFE {safe_link}\n"
        "────────────────\n\n"
        "🗓 今月累計DEX手数料収益\n"
        f"+{fmt_money(mtd_emitted)}\n\n"
        "📈 実効DEX APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "────────────────\n"
        "🎉 当日DEX手数料収益\n"
        f"{fmt_money(emitted_today)}\n\n"
        "📆 1日あたりDEX手数料収益（直近7日平均）\n"
        f"{fmt_money(avg_emitted_day_7d)}\n\n"
        "📊 NFT Positions\n"
        + ("\n".join(nft_lines) if nft_lines else "—")
        + "\n"
    )
    return msg


def build_weekly_message(
    safe_address: str,
    period_end: datetime,
    net_total: float,
    history: List[List[str]],
    nft_lines: List[str],
) -> str:
    week_claimed = sum_last_n_days(history, 7, 4)
    prev_week_claimed = sum_prev_n_days(history, 7, 4)
    avg_claimed_day = week_claimed / 7.0 if week_claimed > 0 else 0.0

    wow_txt = "—"
    if prev_week_claimed > 0:
        wow = ((week_claimed - prev_week_claimed) / prev_week_claimed) * 100
        sign = "+" if wow >= 0 else ""
        wow_txt = f"{sign}{wow:.1f}%"

    emitted_7d = sum_last_n_days(history, 7, 6)
    avg_emitted_7d = emitted_7d / 7.0 if emitted_7d > 0 else 0.0
    apr_7d = (avg_emitted_7d / net_total) * 365 * 100 if net_total > 0 else 0.0

    mtd_emitted = sum_month_to_date(history, period_end, 6)
    all_emitted = sum_all_time(history, 6)

    safe_link = fmt_safe_link(safe_address)

    msg = (
        "🚀 CBC Liquidity Mining — Weekly Settlement\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        safe_link = fmt_safe_link(safe_address)  # 0xB1A76*****89734 の青リンク
        ...
        "────────────────\n\n"
        "🎉 今週確定収益\n"
        f"{fmt_money(week_claimed)}\n"
        f"（前週 {fmt_money(prev_week_claimed)} ／ {wow_txt}）\n\n"
        "📆 1日あたり平均確定収益\n"
        f"{fmt_money(avg_claimed_day)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "────────────────\n"
        "📈 実効DEX APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🗓 今月累計DEX手数料収益\n"
        f"+{fmt_money(mtd_emitted)}\n\n"
        "🏆 ALL-TIME DEX手数料収益\n"
        f"+{fmt_money(all_emitted)}\n\n"
        "────────────────\n"
        "📊 NFT Positions\n"
        + ("\n".join(nft_lines) if nft_lines else "—")
        + "\n"
    )
    return msg

# ================================
# config
# ================================
def load_config():
    path = os.environ.get("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ================================
# Core per-safe compute
# ================================
def compute_today_metrics(
    safe_address: str,
    period_end: datetime,
) -> Tuple[List[dict], List[dict], float, float, float, int, Dict[str, float]]:
    """
    Returns:
      pos_open,
      pos_all,
      net_total_usd,
      claimed_24h_usd,
      unclaimed_usd,
      tx_count_24h,
      claimed_24h_by_nft (USD)
    """
    start_dt = period_end - timedelta(days=1)

    positions_open = fetch_positions(safe_address, active=True)
    positions_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(positions_open)
    pos_exited = _normalize_positions(positions_exited)
    pos_all = pos_open + pos_exited

    net_total = 0.0
    unclaimed = 0.0
    for pos in pos_open:
        net = to_f(calc_net_usd(pos)) or 0.0
        net_total += float(net)
        unclaimed += float(to_f(pos.get("fees_value"), 0.0) or 0.0)

    claimed_24h, tx_24h = calc_claimed_usd_in_window(pos_all, start_dt, period_end)
    claimed_by_nft = calc_claimed_usd_by_nft_in_window(pos_all, start_dt, period_end)

    return pos_open, pos_all, float(net_total), float(claimed_24h), float(unclaimed), int(tx_24h), claimed_by_nft

def get_yesterday_unclaimed_from_history(history: List[List[str]]) -> float:
    if not history:
        return 0.0
    if len(history) >= 2:
        return _row_val(history[-2], 5)
    return 0.0

# ================================
# main
# ================================
def main():
    mode = get_mode()
    print(f"DBG MODE={mode}", flush=True)

    cfg = load_config()
    safes = cfg.get("safes") or []
    if not safes:
        print("config.json: safes is empty", flush=True)
        return

    period_end = get_period_end_jst()

    # Sheets init
    sheets_enabled = True
    ws_log = None
    ws_wide = None
    try:
        client = get_gsheet_client()
        sh = open_sheet(client)
        ws_log = get_log_ws(sh)
        ensure_log_header(ws_log)
        ws_wide = get_daily_wide_ws(sh)
    except Exception as e:
        sheets_enabled = False
        print(f"DBG: Sheets disabled (err={e})", flush=True)

    # Read whole log once
    all_rows: List[List[str]] = []
    if sheets_enabled and ws_log:
        all_rows = read_log_rows(ws_log)

    for s in safes:
        safe_name = (s.get("name") or "NONAME").strip()
        safe_address = (s.get("safe_address") or "").strip()
        chat_id = (s.get("telegram_chat_id") or "").strip()

        if not safe_address:
            print(f"skip: missing safe_address name={safe_name}", flush=True)
            continue
        if not chat_id:
            print(f"skip: missing telegram_chat_id name={safe_name}", flush=True)
            continue

        try:
            # history for this safe (BEFORE write)
            safe_hist = get_safe_history(all_rows, safe_address)

            pos_open, _pos_all, net_total, claimed_24h, unclaimed_today, _tx_24h, claimed_by_nft = compute_today_metrics(
                safe_address, period_end
            )

            # emitted proxy: max(0, Δunclaimed) + claimed
            y_unclaimed = get_yesterday_unclaimed_from_history(safe_hist)
            delta_unclaimed = unclaimed_today - y_unclaimed
            if delta_unclaimed < 0:
                delta_unclaimed = 0.0
            emitted_today = float(delta_unclaimed) + float(claimed_24h)

            # ✅ Sheets: DAILY_LOG（縦）
            if sheets_enabled and ws_log:
                upsert_daily_log_row(
                    ws_log,
                    period_end,
                    safe_name,
                    safe_address,
                    net_total,
                    claimed_24h,
                    unclaimed_today,
                    emitted_today,
                )
                # in-memory append
                all_rows.append([
                    period_end.strftime("%Y-%m-%d %H:%M"),
                    safe_name,
                    safe_address,
                    str(net_total),
                    str(claimed_24h),
                    str(unclaimed_today),
                    str(emitted_today),
                ])
                safe_hist = get_safe_history(all_rows, safe_address)

            # ✅ Sheets: DAILY_WIDE（横）… claimed_24h を格納（既存仕様）
            if sheets_enabled and ws_wide:
                append_daily_wide_numbered(ws_wide, period_end, safe_name, safe_address, claimed_24h)

            # NFT lines (simple, linked, base)
            nft_lines = build_nft_lines_simple(pos_open, claimed_by_nft)

            # Telegram
            if mode == "WEEKLY":
                msg = build_weekly_message(
                    safe_address=safe_address,
                    period_end=period_end,
                    net_total=net_total,
                    history=safe_hist,
                    nft_lines=nft_lines,
                )
                send_telegram(msg, chat_id)
            else:
                msg = build_daily_message(
                    safe_address=safe_address,
                    period_end=period_end,
                    net_total=net_total,
                    emitted_today=emitted_today,
                    history=safe_hist,
                    nft_lines=nft_lines,
                )
                send_telegram(msg, chat_id)

        except Exception as e:
            print(f"error name={safe_name} safe={safe_address}: {e}", flush=True)
            try:
                send_telegram(
                    f"CBC LM ERROR\n\nNAME\n{h(safe_name)}\n\nSAFE\n{h(safe_address)}\n\nERROR\n{h(e)}",
                    chat_id,
                )
            except Exception:
                pass

if __name__ == "__main__":
    main()
