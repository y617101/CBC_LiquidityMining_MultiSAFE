import os
import json
import html
import time
import requests
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

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

def today_jst(now: Optional[datetime] = None) -> date:
    now = now or datetime.now(JST)
    return now.astimezone(JST).date()

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

def get_log_ws(sh):
    tab_name = os.getenv("GOOGLE_SHEET_LOG_TAB", "DAILY_LOG")
    return sh.worksheet(tab_name)

def ensure_log_header(ws):
    """
    Log schema (one row per SAFE per day):
    A: period_end_jst (YYYY-MM-DD HH:MM)
    B: safe_name
    C: safe_address
    D: net_total_usd
    E: claimed_24h_usd
    F: unclaimed_usd
    G: emitted_usd (simple proxy)
    """
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
    # expected "YYYY-MM-DD HH:MM"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=JST)
    except Exception:
        return None

def read_log_rows(ws) -> List[List[str]]:
    values = sheets_call(ws.get_all_values) or []
    if len(values) <= 1:
        return []
    return values[1:]  # without header

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
    """
    If same (period_end, safe_address) already exists -> update.
    else append.
    """
    period_key = period_end.strftime("%Y-%m-%d %H:%M")
    rows = read_log_rows(ws)

    target_row_idx = None
    for i, row in enumerate(rows, start=2):  # sheet row index (header=1)
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
# Fees - claimed in window (USD)
# ================================
def _get_cf_usd_ui_first(cf: dict) -> Optional[float]:
    # UI寄せ（候補を広めに）
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
    """
    Sum claimed fees (USD) in [start_dt, end_dt)
    types: fees-collected / claimed-fees
    De-dup by (tx_hash, type) to avoid open+exited double count
    """
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
    # month start at 1st 09:00 anchor
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
# Build DAILY / WEEKLY messages
# ================================
def build_daily_investor_message(
    safe_name: str,
    safe_address: str,
    period_end: datetime,
    net_total: float,
    claimed_24h: float,
    unclaimed_today: float,
    emitted_today: float,
    history: List[List[str]],
) -> str:
    """
    Investor DAILY:
      - Effective DEX APR (7d avg, emitted-based) with denominator = CURRENT Net
      - MTD emitted
      - ALL-TIME emitted
      - This week claimed + WoW
      - Avg claimed per day (this week)
      - Current Net
    """
    # indices in log row:
    # D net_total(3), E claimed(4), F unclaimed(5), G emitted(6)
    emitted_7d = sum_last_n_days(history, 7, 6)
    avg_emitted_7d = emitted_7d / 7.0 if emitted_7d > 0 else 0.0
    apr_7d = (avg_emitted_7d / net_total) * 365 * 100 if net_total > 0 else 0.0

    mtd_emitted = sum_month_to_date(history, period_end, 6)
    all_emitted = sum_all_time(history, 6)

    week_claimed = sum_last_n_days(history, 7, 4)
    prev_week_claimed = sum_prev_n_days(history, 7, 4)
    wow_pct = None
    if prev_week_claimed > 0:
        wow_pct = ((week_claimed - prev_week_claimed) / prev_week_claimed) * 100

    avg_claimed_day = week_claimed / 7.0 if week_claimed > 0 else 0.0

    # ALL-TIME start label = first recorded day (facts only)
    fst = first_record_dt(history)
    all_from = fst.strftime("%Y-%m-%d") if fst else "N/A"

    wow_txt = "—"
    if wow_pct is not None:
        sign = "+" if wow_pct >= 0 else ""
        wow_txt = f"{sign}{wow_pct:.1f}%"

    msg = (
        "CBC Liquidity Mining — Daily\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"SAFE {safe_address}\n"
        "────────────────\n\n"
        "📈 実効DEX APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🗓 今月累計発生収益（MTD）\n"
        f"+{fmt_money(mtd_emitted)}\n\n"
        "🏆 ALL-TIME発生収益\n"
        f"+{fmt_money(all_emitted)}\n"
        f"（開始: {all_from} / 初回記録日ベース）\n\n"
        "────────────────\n"
        "🎉 今週確定獲得額\n"
        f"{fmt_money(week_claimed)}  （前週比 {wow_txt}）\n\n"
        "📆 1日あたり平均確定額\n"
        f"{fmt_money(avg_claimed_day)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n"
    )
    return msg

def build_weekly_settlement_message(
    safe_address: str,
    period_end: datetime,
    net_total: float,
    history: List[List[str]],
) -> str:
    """
    WEEKLY settlement:
      - This week claimed (7d)
      - Previous week claimed + WoW
      - Avg claimed/day
      - (optional) show 7d APR as reference
    """
    week_claimed = sum_last_n_days(history, 7, 4)
    prev_week_claimed = sum_prev_n_days(history, 7, 4)
    avg_claimed_day = week_claimed / 7.0 if week_claimed > 0 else 0.0

    wow_txt = "—"
    if prev_week_claimed > 0:
        wow = ((week_claimed - prev_week_claimed) / prev_week_claimed) * 100
        sign = "+" if wow >= 0 else ""
        wow_txt = f"{sign}{wow:.1f}%"

    # reference APR (emitted-based, 7d avg, current net)
    emitted_7d = sum_last_n_days(history, 7, 6)
    avg_emitted_7d = emitted_7d / 7.0 if emitted_7d > 0 else 0.0
    apr_7d = (avg_emitted_7d / net_total) * 365 * 100 if net_total > 0 else 0.0

    msg = (
        "CBC Liquidity Mining — Weekly Settlement\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"SAFE {safe_address}\n"
        "────────────────\n\n"
        "🎉 今週確定獲得額\n"
        f"{fmt_money(week_claimed)}\n"
        f"（前週 {fmt_money(prev_week_claimed)} ／ {wow_txt}）\n\n"
        "📆 1日あたり平均確定額（今週）\n"
        f"{fmt_money(avg_claimed_day)}\n\n"
        "────────────────\n"
        "📈 実効DEX APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n"
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
def compute_today_metrics(safe_address: str, period_end: datetime) -> Tuple[float, float, float, float, int]:
    """
    Returns:
      net_total_usd,
      claimed_24h_usd,
      unclaimed_usd (fees_value sum on active positions),
      emitted_usd (placeholder computed later with yesterday unclaimed),
      tx_count_24h
    """
    start_dt = period_end - timedelta(days=1)

    positions_open = fetch_positions(safe_address, active=True)
    positions_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(positions_open)
    pos_exited = _normalize_positions(positions_exited)
    pos_all = pos_open + pos_exited

    # Net total from active positions only (current state)
    net_total = 0.0
    unclaimed = 0.0
    for pos in pos_open:
        net = to_f(calc_net_usd(pos)) or 0.0
        net_total += float(net)
        unclaimed += float(to_f(pos.get("fees_value"), 0.0) or 0.0)

    claimed_24h, tx_24h = calc_claimed_usd_in_window(pos_all, start_dt, period_end)

    # emitted computed later after we load yesterday unclaimed from sheets
    return float(net_total), float(claimed_24h), float(unclaimed), 0.0, int(tx_24h)

def get_yesterday_unclaimed_from_history(history: List[List[str]]) -> float:
    """
    Uses the most recent previous row's unclaimed_usd.
    """
    if not history:
        return 0.0
    # last row is today if already written; we want previous row
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

    # Sheets init (optional; if missing env, will raise -> handled)
    sheet_client = None
    sh = None
    ws_log = None
    sheets_enabled = True
    try:
        sheet_client = get_gsheet_client()
        sh = open_sheet(sheet_client)
        ws_log = get_log_ws(sh)
        ensure_log_header(ws_log)
    except Exception as e:
        sheets_enabled = False
        print(f"DBG: Sheets disabled (err={e})", flush=True)

    # Read whole log once (reduce quota)
    all_rows = []
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
            # history for this safe
            safe_hist = get_safe_history(all_rows, safe_address)

            net_total, claimed_24h, unclaimed_today, _, tx_24h = compute_today_metrics(safe_address, period_end)

            # emitted proxy (facts: uses unclaimed USD delta + claimed)
            y_unclaimed = get_yesterday_unclaimed_from_history(safe_hist)
            delta_unclaimed = unclaimed_today - y_unclaimed
            if delta_unclaimed < 0:
                delta_unclaimed = 0.0
            emitted_today = float(delta_unclaimed) + float(claimed_24h)

            # upsert log (so future runs have data)
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
                # refresh safe history in-memory (append today's row locally)
                # (simple: re-read for correctness if you want; we keep it light)
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

            # Sunday: WEEKLY only
            if mode == "WEEKLY":
                msg = build_weekly_settlement_message(
                    safe_address=safe_address,
                    period_end=period_end,
                    net_total=net_total,
                    history=safe_hist,
                )
                send_telegram(msg, chat_id)
            else:
                msg = build_daily_investor_message(
                    safe_name=safe_name,
                    safe_address=safe_address,
                    period_end=period_end,
                    net_total=net_total,
                    claimed_24h=claimed_24h,
                    unclaimed_today=unclaimed_today,
                    emitted_today=emitted_today,
                    history=safe_hist,
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
