import os
import json
import html
import time
import csv
import requests

print("DBG BOOT MARKER: 2026-02-24-AAAA", flush=True)

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# ================================
# Constants
# ================================
JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"

WETH_ADDR = "0x4200000000000000000000000000000000000006".lower()
USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".lower()

# ================================
# Small helpers
# ================================
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

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

def dbg(*args):
    if _env("DEBUG", "0") == "1":
        print(*args, flush=True)

def _is_true(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in ("true", "1", "yes", "y")

def build_basescan_addr_link(addr: str) -> str:
    a = (addr or "").strip()
    return f"https://basescan.org/address/{a}"

def mask_safe_addr(addr: str) -> str:
    a = (addr or "").strip()
    if len(a) <= (2 + 5 + 4):
        return a
    return f"{a[:2+5]}*****{a[-4:]}"

def fmt_safe_link(addr: str) -> str:
    url = build_basescan_addr_link(addr)
    label = mask_safe_addr(addr)
    return f'<a href="{h(url)}">{h(label)}</a>'

def build_uniswap_link_base(nft_id: str) -> str:
    return f"https://app.uniswap.org/positions/v3/base/{nft_id}"

def _to_ts_sec(ts):
    try:
        if ts is None:
            return None

        if isinstance(ts, (int, float)):
            ts_i = int(ts)
            if ts_i > 10_000_000_000:
                ts_i //= 1000
            return ts_i

        s = str(ts).strip()
        if not s:
            return None

        if s.isdigit():
            ts_i = int(s)
            if ts_i > 10_000_000_000:
                ts_i //= 1000
            return ts_i

        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def ts_to_dt(ts):
    if ts is None:
        return None

    if isinstance(ts, (int, float)):
        x = float(ts)
        if x > 1e12:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=JST)

    if isinstance(ts, str):
        s = ts.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).astimezone(JST)
        except Exception:
            return None

    return None

def _get_tx_hash(cf: dict) -> str:
    return str(
        cf.get("tx_hash")
        or cf.get("txHash")
        or cf.get("transaction_hash")
        or cf.get("transactionHash")
        or ""
    ).strip()

# ================================
# Cash flow helpers
# ================================
def _norm_cf_type(t) -> str:
    s = str(t or "").strip().lower()
    s = s.replace("_", "-").replace(" ", "-")
    return s

# alias (legacy)
norm_cf_type = _norm_cf_type

def _is_claimed_type(cf_type) -> bool:
    t = _norm_cf_type(cf_type)
    return t in (
        "fees-collected",
        "claimed-fees",
        "fee-collected",
        "feescollected",
        "claimedfees",
    )

def is_claimed_type(cf_type) -> bool:
    return _is_claimed_type(cf_type)

def _get_cf_usd(cf: dict) -> float:
    def _f(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    prices = cf.get("prices") or {}
    p0 = _f(((prices.get("token0") or {}).get("usd")), 0.0)
    p1 = _f(((prices.get("token1") or {}).get("usd")), 0.0)

    a0 = cf.get("amount0")
    a1 = cf.get("amount1")
    if a0 is None:
        a0 = cf.get("collected_fees_token0")
    if a1 is None:
        a1 = cf.get("collected_fees_token1")

    a0 = _f(a0, 0.0)
    a1 = _f(a1, 0.0)

    if (p0 > 0 or p1 > 0) and (a0 != 0.0 or a1 != 0.0):
        return max(0.0, a0 * p0 + a1 * p1)

    for k in ("usd", "usd_value", "value_usd", "amount_usd", "valueUsd", "amountUsd"):
        v = cf.get(k)
        vv = _f(v, 0.0)
        if vv > 0:
            return vv

    return 0.0

def pick_confirmed_cf(cash_flows: List[dict], period_start: datetime, period_end: datetime) -> List[dict]:
    def _to_f(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    rows: List[dict] = []
    passed = 0

    for cf in (cash_flows or []):
        if not isinstance(cf, dict):
            continue

        t_norm = _norm_cf_type(cf.get("type"))
        if not _is_claimed_type(t_norm):
            continue

        dt = (
            ts_to_dt(cf.get("timestamp"))
            or ts_to_dt(cf.get("ts"))
            or ts_to_dt(cf.get("time"))
            or ts_to_dt(cf.get("created_at"))
            or ts_to_dt(cf.get("date"))
        )

        if not dt:
            continue
        if not (period_start <= dt < period_end):
            continue

        passed += 1

        txh = (_get_tx_hash(cf) or "").lower().strip()
        nft = str(cf.get("nft_id") or cf.get("token_id") or cf.get("_pos_nft_id") or "").strip()

        prices = cf.get("prices") or {}
        p0 = _to_f(((prices.get("token0") or {}).get("usd")))
        p1 = _to_f(((prices.get("token1") or {}).get("usd")))

        a0 = cf.get("amount0")
        a1 = cf.get("amount1")
        a0 = _to_f(a0 if a0 is not None else cf.get("collected_fees_token0"))
        a1 = _to_f(a1 if a1 is not None else cf.get("collected_fees_token1"))

        usd = _to_f(cf.get("usd"))
        if usd <= 0:
            if p0 > 0 or p1 > 0:
                usd = (a0 * p0) + (a1 * p1)
        if usd < 0:
            usd = 0.0

        # WETH/USDC 推定（priceで推定）
        weth_amt = 0.0
        usdc_amt = 0.0
        if p0 > 100 and 0.9 <= p1 <= 1.1:
            weth_amt = a0
            usdc_amt = a1
        elif p1 > 100 and 0.9 <= p0 <= 1.1:
            weth_amt = a1
            usdc_amt = a0
        else:
            if p0 >= p1:
                weth_amt = a0
                usdc_amt = a1
            else:
                weth_amt = a1
                usdc_amt = a0

        rows.append({
            "usd": float(usd or 0.0),
            "amount_weth": float(weth_amt or 0.0),
            "amount_usdc": float(usdc_amt or 0.0),
            "type": cf.get("type") or "",
            "tx_hash": txh,
            "nft_id": nft,
            "raw": cf,
        })

    # 重複排除（claimed-fees優先）
    grouped: Dict[tuple, List[dict]] = {}
    for r in rows:
        tx = (r.get("tx_hash") or "").strip()
        nft = (r.get("nft_id") or "").strip()
        raw = r.get("raw") or {}
        fallback = str(raw.get("timestamp") or raw.get("date") or "")
        key = (tx, nft if nft else f"__no_nft__{fallback}")
        grouped.setdefault(key, []).append(r)

    picked: List[dict] = []
    for _, arr in grouped.items():
        if not arr:
            continue
        claimed = [item for item in arr if _norm_cf_type(item.get("type")) == "claimed-fees"]
        target = claimed if claimed else arr
        best = max(target, key=lambda item: float(item.get("usd") or 0.0))
        picked.append(best)

    dbg("DBG pick_confirmed_cf passed/rows:", passed, len(rows))
    dbg("DBG pick_confirmed_cf grouped/picked:", len(grouped), len(picked))
    return picked

# ================================
# Telegram
# ================================
def send_telegram(text: str, chat_id: str):
    token = _env("TG_BOT_TOKEN", "")
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

def send_telegram_file(file_path: str, chat_id: str, caption: str = ""):
    token = _env("TG_BOT_TOKEN", "")
    if not token:
        print("Telegram ENV missing: TG_BOT_TOKEN", flush=True)
        return
    if not chat_id:
        print("Telegram chat_id missing", flush=True)
        return

    url = f"https://api.telegram.org/bot{token}/sendDocument"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"file not found: {file_path}")

    with open(file_path, "rb") as f:
        files = {"document": (os.path.basename(file_path), f)}
        data = {
            "chat_id": chat_id,
            "caption": str(caption or ""),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        r = requests.post(url, data=data, files=files, timeout=60)
        dbg("Telegram file status:", r.status_code, r.text[:200])
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
    sheet_id = _env("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("ENV missing: GOOGLE_SHEET_ID")
    return client.open_by_key(sheet_id)

def get_weekly_log_ws(sh):
    ws = sh.worksheet(_env("GOOGLE_SHEET_WEEKLY_TAB", "WEEKLY_LOG"))
    existing = sheets_call(ws.get_all_values)
    if not existing:
        sheets_call(
            ws.update,
            "A1:F1",
            [["week_ending", "safe_name", "safe_address", "weth", "usdc", "usd_fix"]],
            value_input_option="USER_ENTERED",
        )
    return ws

def append_weekly_log_row_once(
    ws,
    week_ending,
    safe_name,
    safe_address,
    confirmed_weth,
    confirmed_usdc,
    confirmed_usd_fix,
):
    week_key = week_ending.strftime("%Y-%m-%d %H:%M")
    safe_norm = str(safe_address).strip().lower()

    existing = sheets_call(ws.get_all_values)  # header含む
    if not existing:
        sheets_call(
            ws.update,
            "A1:F1",
            [["week_ending", "safe_name", "safe_address", "weth", "usdc", "usd_fix"]],
            value_input_option="USER_ENTERED",
        )
        existing = sheets_call(ws.get_all_values)

    target_row = None
    for i, row in enumerate(existing[1:], start=2):
        if len(row) < 3:
            continue
        wk = str(row[0]).strip()
        sa = str(row[2]).strip().lower()
        if wk == week_key and sa == safe_norm:
            target_row = i
            break

    out = [
        week_key,
        safe_name,
        safe_address,
        float(confirmed_weth),
        float(confirmed_usdc),
        float(confirmed_usd_fix),
    ]

    if target_row is None:
        sheets_call(ws.append_row, out, value_input_option="USER_ENTERED")
        print(f"DBG: WEEKLY_LOG appended {safe_name} {week_key}", flush=True)
    else:
        sheets_call(ws.update, f"A{target_row}:F{target_row}", [out], value_input_option="USER_ENTERED")
        print(f"DBG: WEEKLY_LOG updated {safe_name} {week_key}", flush=True)

def get_config_recipients_ws(sh):
    tab_name = _env("GOOGLE_SHEET_CONFIG_TAB", "CONFIG_RECIPIENTS")
    return sh.worksheet(tab_name)

def load_active_recipients_for_safe(sh, safe_name: str):
    """
    CONFIG_RECIPIENTS:
      safe_name, recipient_id, name, address, pct, active(TRUE/FALSE)
    pct は「10」「4」「70」みたいな %値として扱う（合計90など）
    """
    ws = get_config_recipients_ws(sh)
    rows = sheets_call(ws.get_all_records) or []

    result = []
    for r in rows:
        if str(r.get("safe_name", "")).strip() != safe_name:
            continue
        if not _is_true(r.get("active")):
            continue

        addr = str(r.get("address") or "").strip()
        if not addr:
            # 空白アドレスは事故防止で除外
            continue

        pct_raw = str(r.get("pct", "")).replace("%", "").strip()
        try:
            pct = float(pct_raw)  # percent
        except Exception:
            pct = 0.0

        if pct <= 0:
            # 0%は除外（事故防止）
            continue

        result.append({
            "recipient_id": str(r.get("recipient_id") or "").strip(),
            "name": str(r.get("name") or "").strip(),
            "address": addr,
            "pct": pct,  # percent
        })

    return result

# ---- WEEKLY_PAYOUTS sheet ----
def get_weekly_payouts_ws(sh):
    tab = _env("GOOGLE_SHEET_PAYOUTS_TAB", "WEEKLY_PAYOUTS")
    ws = sh.worksheet(tab)
    existing = sheets_call(ws.get_all_values)
    if not existing:
        sheets_call(
            ws.update,
            "A1:I1",
            [[
                "week_ending",
                "safe_name",
                "safe_address",
                "recipient_id",
                "name",
                "address",
                "pct",
                "amount_usdc",
                "created_at_jst",
            ]],
            value_input_option="USER_ENTERED",
        )
    return ws

def append_weekly_payout_rows_once(ws, rows: List[List], week_ending: datetime, safe_address: str):
    """
    dedup key: week_ending + safe_address + recipient_id
    ※ rows は header無しのリスト行
    """
    existing = sheets_call(ws.get_all_values) or []
    idx = set()
    for r in existing[1:]:
        if len(r) < 4:
            continue
        wk = str(r[0]).strip()
        sa = str(r[2]).strip().lower()
        rid = str(r[3]).strip().lower()
        idx.add((wk, sa, rid))

    new_rows = []
    for r in rows:
        if not isinstance(r, list) or len(r) < 4:
            continue
        wk = str(r[0]).strip()
        sa = str(r[2]).strip().lower()
        rid = str(r[3]).strip().lower()
        key = (wk, sa, rid)
        if key in idx:
            continue
        new_rows.append(r)

    if new_rows:
        sheets_call(ws.append_rows, new_rows, value_input_option="USER_ENTERED")
        print(f"DBG WEEKLY_PAYOUTS appended rows={len(new_rows)}", flush=True)
    else:
        print("DBG WEEKLY_PAYOUTS no new rows (dedup)", flush=True)

# ================================
# Revert API (robust normalize)
# ================================
def fetch_positions(safe: str, active: bool = True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false"}

    # optional: with-v4 (OFF by default to avoid instability)
    if _env("WITH_V4", "0") == "1":
        params["with-v4"] = "true"

    retries = 5
    delay = 1

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code >= 500:
                raise Exception(f"Server {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            dbg("DBG fetch_positions retry", attempt + 1, "error=", e)
            time.sleep(delay)
            delay *= 2

    dbg("DBG FINAL FAIL - returning empty")
    return []

def _normalize_positions(resp) -> List[dict]:
    if isinstance(resp, list):
        return [p for p in resp if isinstance(p, dict)]

    if isinstance(resp, dict):
        for k in ("positions", "data", "result"):
            v = resp.get(k)
            if isinstance(v, list):
                return [p for p in v if isinstance(p, dict)]
        v = resp.get("positions")
        if isinstance(v, dict):
            vv = v.get("data")
            if isinstance(vv, list):
                return [p for p in vv if isinstance(p, dict)]
    return []

# ================================
# Net USD (underlying - debt)
# ================================
def extract_debt_usd(pos) -> float:
    cfs = pos.get("cash_flows") or []
    if not isinstance(cfs, list):
        return 0.0

    latest_td = None
    latest_ts = -1
    for cf in cfs:
        if not isinstance(cf, dict):
            continue
        t = _lower(cf.get("type"))
        if t not in ("lendor-borrow", "lendor-repay"):
            continue
        td = to_f(cf.get("total_debt"))
        ts = _to_ts_sec(cf.get("timestamp")) or 0
        if td is not None and ts >= latest_ts:
            latest_ts = ts
            latest_td = td

    if latest_td is not None:
        return max(float(latest_td), 0.0)

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
# APR (Revert)
# ================================
def get_revert_fee_apr(pos: dict) -> float:
    v = to_f(pos.get("fee_apr"))
    if v is not None:
        return float(v)

    perf = pos.get("performance") or {}
    if isinstance(perf, dict):
        v = to_f(perf.get("fee_apr"))
        if v is not None:
            return float(v)
        hodl = perf.get("hodl") or {}
        if isinstance(hodl, dict):
            v = to_f(hodl.get("fee_apr"))
            if v is not None:
                return float(v)

    return 0.0

# ================================
# Time (JST 00:00 close; send at 09:00)
# ================================
def get_period_end_jst(now: Optional[datetime] = None) -> datetime:
    if now is None:
        now = datetime.now(JST)
    else:
        now = now.astimezone(JST) if now.tzinfo else now.replace(tzinfo=JST)

    anchor = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=1)
    return anchor

def get_weekly_period_end_jst(now: Optional[datetime] = None) -> datetime:
    if now is None:
        now = datetime.now(JST)
    else:
        now = now.astimezone(JST) if now.tzinfo else now.replace(tzinfo=JST)

    days_since_sun = (now.weekday() - 6) % 7
    sun_0 = (now - timedelta(days=days_since_sun)).replace(hour=0, minute=0, second=0, microsecond=0)
    if now < sun_0:
        sun_0 -= timedelta(days=7)
    return sun_0

def pick_mode_auto(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(JST)
    return "WEEKLY" if now.weekday() == 6 else "DAILY"

def get_mode() -> str:
    raw = _env("REPORT_MODE", "").upper()
    if raw in ("DAILY", "WEEKLY"):
        return raw
    return pick_mode_auto()

# ================================
# NFT lines (UI rule)
# ================================
def build_nft_lines_revert_apr(pos_open: List[dict]) -> List[str]:
    lines: List[str] = []
    for pos in (pos_open or []):
        nft_id = str(pos.get("nft_id") or "").strip()
        if not nft_id:
            continue

        status = "RANGE OUT" if pos.get("in_range") is False else "ACTIVE"
        net = float(to_f(calc_net_usd(pos), 0.0) or 0.0)
        apr = float(get_revert_fee_apr(pos) or 0.0)

        url = build_uniswap_link_base(nft_id)
        nft_link = f'<a href="{h(url)}">{h(nft_id)}</a>'

        lines.append(
            f"{nft_link} | {h(status)}\n"
            f"Net {fmt_money(net)} | APR {fmt_pct(apr)}"
        )
    return lines

# ================================
# Messages (Daily / Weekly)
# ================================
def build_daily_message(
    safe_address: str,
    period_end: datetime,
    net_total: float,
    pos_open: List[dict],
    avg_confirmed_7d_usd: float,
    mtd_confirmed_usd: float,
    mtd_weth: float,
    mtd_usdc: float,
) -> str:
    uncollected_now_value = 0.0
    for p in (pos_open or []):
        try:
            if p.get("fees_value") is not None:
                uncollected_now_value += float(p.get("fees_value") or 0.0)
            elif p.get("uncollected_fees_value") is not None:
                uncollected_now_value += float(p.get("uncollected_fees_value") or 0.0)
        except Exception:
            pass

    apr_7d = (avg_confirmed_7d_usd / net_total) * 365.0 * 100.0 if net_total > 0 else 0.0

    nft_lines = build_nft_lines_revert_apr(pos_open)
    safe_link = fmt_safe_link(safe_address)

    msg = (
        "🚀 CBC Liquidity Mining — Daily\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST (LIVE)\n"
        f"SAFE {safe_link}\n"
        "──────────\n\n"
        "🗓 現在DEX手数料（Value）\n"
        f"{fmt_money(uncollected_now_value)}\n\n"
        "📈 推定戦略APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "──────────\n"
        "🎉 月間累計 確定DEX手数料収益\n"
        f"{mtd_weth:.6f} ETH  {mtd_usdc:,.2f} USDC\n"
        f"{fmt_money(mtd_confirmed_usd)}（Value）\n\n"
        "📆 1日あたり確定DEX手数料収益（直近7日平均）\n"
        f"{fmt_money(avg_confirmed_7d_usd)}\n\n"
        "──────────\n"
        "📊 NFT Positions\n"
        + ("\n\n".join(nft_lines) if nft_lines else "—")
        + "\n"
    )
    return msg

def build_weekly_message(
    safe_address: str,
    period_end: datetime,
    net_total: float,
    week_claimed: float,
    prev_week_claimed: float,
    mtd_confirmed: float,
    all_confirmed: float,
    week_weth: float = 0.0,
    week_usdc: float = 0.0,
    mtd_weth: float = 0.0,
    mtd_usdc: float = 0.0,
    all_weth: float = 0.0,
    all_usdc: float = 0.0,
    pos_open: Optional[List[dict]] = None,
) -> str:
    pos_open = pos_open or []

    avg_claimed_day = week_claimed / 7.0 if week_claimed > 0 else 0.0

    wow_txt = "—"
    if prev_week_claimed > 0:
        wow = ((week_claimed - prev_week_claimed) / prev_week_claimed) * 100.0
        sign = "+" if wow >= 0 else ""
        wow_txt = f"{sign}{wow:.1f}%"

    safe_link = fmt_safe_link(safe_address)
    apr_7d = (avg_claimed_day / net_total) * 365.0 * 100.0 if net_total > 0 else 0.0

    nft_lines = build_nft_lines_revert_apr(pos_open)

    msg = (
        "🚀 CBC Liquidity Mining — Weekly Settlement\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST（FIX）\n"
        f"SAFE {safe_link}\n"
        "──────────\n\n"
        "🎉 今週 確定DEX手数料収益\n"
        f"{week_weth:.6f} ETH\n"
        f"{week_usdc:,.2f} USDC\n"
        f"{fmt_money(week_claimed)}\n"
        f"（前週 {fmt_money(prev_week_claimed)} ／ {wow_txt}）\n\n"
        "📆 1日あたり平均確定手数料収益\n"
        f"{fmt_money(avg_claimed_day)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "──────────\n"
        "📈 推定戦略 APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🗓 今月累計 確定DEX手数料収益\n"
        f"{mtd_weth:.6f} ETH  {mtd_usdc:,.2f} USDC\n"
        f"{fmt_money(mtd_confirmed)}\n\n"
        "🏆 ALL-TIME 確定手数料収益\n"
        f"{all_weth:.6f} ETH  {all_usdc:,.2f} USDC\n"
        f"{fmt_money(all_confirmed)}\n\n"
        "──────────\n"
        "📊 NFT Positions\n"
        + ("\n\n".join(nft_lines) if nft_lines else "—")
        + "\n"
    )
    return msg

# ================================
# config
# ================================
def load_config():
    path = _env("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ================================
# Daily compute
# ================================
def compute_daily_revert_metrics(
    safe_address: str,
    period_end: datetime,
) -> Tuple[List[dict], float, float, float, float, float, float]:

    resp_open = fetch_positions(safe_address, active=True)
    resp_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(resp_open)
    pos_exited = _normalize_positions(resp_exited)
    pos_all = (pos_open or []) + (pos_exited or [])

    net_total = 0.0
    for pos in (pos_open or []):
        net_total += float(to_f(calc_net_usd(pos), 0.0) or 0.0)

    uncollected_now_value = 0.0
    for p in (pos_open or []):
        try:
            if p.get("fees_value") is not None:
                uncollected_now_value += float(p.get("fees_value") or 0.0)
            elif p.get("uncollected_fees_value") is not None:
                uncollected_now_value += float(p.get("uncollected_fees_value") or 0.0)
        except Exception:
            pass

    cash_flows_all: List[dict] = []
    for pos in (pos_all or []):
        pos_nft = str(pos.get("nft_id") or "").strip()
        cfs = pos.get("cash_flows") or []
        if not isinstance(cfs, list):
            continue
        for cf in cfs:
            if isinstance(cf, dict) and pos_nft:
                if not (cf.get("nft_id") or cf.get("token_id")):
                    cf["_pos_nft_id"] = pos_nft
            cash_flows_all.append(cf)

    start_7d = period_end - timedelta(days=7)
    rows_7d = pick_confirmed_cf(cash_flows_all, start_7d, period_end)
    confirmed_7d = float(sum((r.get("usd") or 0.0) for r in rows_7d))
    avg_confirmed_7d_usd = confirmed_7d / 7.0 if confirmed_7d > 0 else 0.0

    pe = period_end.astimezone(JST)
    month_start = datetime(pe.year, pe.month, 1, 0, 0, 0, tzinfo=JST)

    mtd_rows = pick_confirmed_cf(cash_flows_all, month_start, period_end)
    mtd_confirmed_usd = float(sum((r.get("usd") or 0.0) for r in mtd_rows))
    mtd_weth = float(sum((r.get("amount_weth") or 0.0) for r in mtd_rows))
    mtd_usdc = float(sum((r.get("amount_usdc") or 0.0) for r in mtd_rows))

    return (
        pos_open,
        float(net_total),
        float(uncollected_now_value),
        float(avg_confirmed_7d_usd),
        float(mtd_confirmed_usd),
        float(mtd_weth),
        float(mtd_usdc),
    )

# ================================
# Weekly compute
# ================================
def compute_weekly_confirmed_metrics(
    safe_address: str,
    period_end: datetime,
) -> Tuple[
    List[dict], float, Dict[str, float],
    float, float,
    float, float,
    float, float,
    float, float,
    float, float
]:
    start_this = period_end - timedelta(days=7)
    end_this = period_end
    start_prev = period_end - timedelta(days=14)
    end_prev = period_end - timedelta(days=7)

    resp_open = fetch_positions(safe_address, active=True)
    resp_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(resp_open)
    pos_exited = _normalize_positions(resp_exited)
    pos_all = pos_open + pos_exited

    cash_flows_all: List[dict] = []
    for pos in (pos_all or []):
        pos_nft = str(pos.get("nft_id") or "").strip()
        cfs = pos.get("cash_flows") or []
        if not isinstance(cfs, list):
            continue
        for cf in cfs:
            if isinstance(cf, dict) and pos_nft:
                if not (cf.get("nft_id") or cf.get("token_id")):
                    cf["_pos_nft_id"] = pos_nft
            cash_flows_all.append(cf)

    net_total = 0.0
    for pos in (pos_open or []):
        net_total += float(to_f(calc_net_usd(pos), 0.0) or 0.0)

    week_rows = pick_confirmed_cf(cash_flows_all, start_this, end_this)
    week_weth = float(sum(r.get("amount_weth", 0.0) for r in week_rows))
    week_usdc = float(sum(r.get("amount_usdc", 0.0) for r in week_rows))
    week_total = float(sum(r.get("usd", 0.0) for r in week_rows))

    prev_rows = pick_confirmed_cf(cash_flows_all, start_prev, end_prev)
    prev_week_total = float(sum(r.get("usd", 0.0) for r in prev_rows))

    month_start = datetime(
        period_end.astimezone(JST).year,
        period_end.astimezone(JST).month,
        1, 0, 0,
        tzinfo=JST
    )
    mtd_rows = pick_confirmed_cf(cash_flows_all, month_start, period_end)
    mtd_confirmed = float(sum((r.get("usd") or 0.0) for r in mtd_rows))
    mtd_weth = float(sum((r.get("amount_weth") or 0.0) for r in mtd_rows))
    mtd_usdc = float(sum((r.get("amount_usdc") or 0.0) for r in mtd_rows))

    all_start = datetime(2020, 1, 1, tzinfo=JST)
    all_rows = pick_confirmed_cf(cash_flows_all, all_start, period_end)
    all_confirmed = float(sum((r.get("usd") or 0.0) for r in all_rows))
    all_weth = float(sum((r.get("amount_weth") or 0.0) for r in all_rows))
    all_usdc = float(sum((r.get("amount_usdc") or 0.0) for r in all_rows))

    confirmed_by_nft_7d: Dict[str, float] = {}
    for r in week_rows:
        nft = str(r.get("nft_id") or "").strip()
        if not nft:
            continue
        confirmed_by_nft_7d[nft] = float(confirmed_by_nft_7d.get(nft, 0.0) or 0.0) + float(r.get("usd") or 0.0)

    return (
        pos_open,
        float(net_total),
        confirmed_by_nft_7d,
        float(week_total),
        float(prev_week_total),
        float(mtd_confirmed),
        float(all_confirmed),
        float(week_weth),
        float(week_usdc),
        float(mtd_weth),
        float(mtd_usdc),
        float(all_weth),
        float(all_usdc),
    )

# ================================
# Payout CSV
# ================================
def build_weekly_payout_rows_with_safe_remainder(
    week_ending: datetime,
    safe_name: str,
    safe_address: str,
    confirmed_usd_fix: float,
    recipients: List[dict],
) -> Tuple[List[List], float, float]:
    """
    recipients: [{recipient_id,name,address,pct(%)}]
    pct is percent number (e.g. 70, 10)
    Returns: rows(for CSV/sheet), pct_sum, remain_usd
    """
    pct_sum = round(sum(float(r.get("pct", 0.0) or 0.0) for r in recipients), 6)
    payout_base = round(float(confirmed_usd_fix) * (pct_sum / 100.0), 2)
    remain = round(float(confirmed_usd_fix) - payout_base, 2)

    created_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    week_key = week_ending.strftime("%Y-%m-%d %H:%M")

    rows = []
    for r in recipients:
        pct = float(r.get("pct", 0.0) or 0.0)
        amt = round(float(confirmed_usd_fix) * (pct / 100.0), 6)
        rows.append([
            week_key,
            safe_name,
            safe_address,
            str(r.get("recipient_id") or ""),
            str(r.get("name") or ""),
            str(r.get("address") or ""),
            pct,
            amt,
            created_at,
        ])

    # SAFE内残し（system分など）※送金CSVからは除外するが、記録は残せる
    if remain > 0:
        rows.append([
            week_key,
            safe_name,
            safe_address,
            "SAFE_REMAINDER",
            "SAFE_REMAINDER",
            safe_address,
            round(100.0 - pct_sum, 6),
            round(remain, 6),
            created_at,
        ])

    return rows, pct_sum, remain

def write_csv(rows: List[List], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "week_ending",
            "safe_name",
            "safe_address",
            "recipient_id",
            "name",
            "address",
            "pct",
            "amount_usdc",
            "created_at_jst",
        ])
        for r in rows:
            w.writerow(r)
    return path

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

    period_end = get_weekly_period_end_jst() if mode == "WEEKLY" else get_period_end_jst()
    print("DBG period_end JST:", period_end.strftime("%Y-%m-%d %H:%M"), flush=True)

    csv_hub_chat_id = _env("CSV_HUB_CHAT_ID", "@csvhub")  # ← ここで集約先をENV化

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
            if mode == "WEEKLY":
                (
                    pos_open, net_total, _by_nft_7d,
                    week_claimed, prev_week_claimed,
                    mtd_confirmed, all_confirmed,
                    week_weth, week_usdc,
                    mtd_weth, mtd_usdc,
                    all_weth, all_usdc,
                ) = compute_weekly_confirmed_metrics(safe_address, period_end)

                msg = build_weekly_message(
                    safe_address=safe_address,
                    period_end=period_end,
                    net_total=net_total,
                    week_claimed=week_claimed,
                    prev_week_claimed=prev_week_claimed,
                    mtd_confirmed=mtd_confirmed,
                    all_confirmed=all_confirmed,
                    week_weth=week_weth,
                    week_usdc=week_usdc,
                    mtd_weth=mtd_weth,
                    mtd_usdc=mtd_usdc,
                    all_weth=all_weth,
                    all_usdc=all_usdc,
                    pos_open=pos_open,
                )

                # Sheets: WEEKLY_LOG
                client = get_gsheet_client()
                sh = open_sheet(client)
                ws_weekly_log = get_weekly_log_ws(sh)
                append_weekly_log_row_once(
                    ws_weekly_log,
                    week_ending=period_end,
                    safe_name=safe_name,
                    safe_address=safe_address,
                    confirmed_weth=week_weth,
                    confirmed_usdc=week_usdc,
                    confirmed_usd_fix=week_claimed,
                )

                # Weekly message
                send_telegram(msg, chat_id=chat_id)

                # payout CSV (optional)
                if _env("PAYOUT_CSV", "0") == "1":
                    recipients = load_active_recipients_for_safe(sh, safe_name=safe_name)
                    payout_rows, pct_sum, remain = build_weekly_payout_rows_with_safe_remainder(
                        week_ending=period_end,
                        safe_name=safe_name,
                        safe_address=safe_address,
                        confirmed_usd_fix=week_claimed,
                        recipients=recipients,
                    )

                    csv_name = f"payout_{safe_name}_{period_end.strftime('%Y-%m-%d')}.csv"

                    # columns: [week_ending, safe_name, safe_address, recipient_id, name, address, pct, amount_usdc, created_at_jst]
                    RIDX = 3  # recipient_id
                    AMTIDX = 7  # amount_usdc ✅（未定義で落ちないように固定）

                    # ✅ 送金CSVは「SAFE_REMAINDER除外」+「0円除外」+「行の形チェック」
                    transfer_rows = [
                        r for r in payout_rows
                        if isinstance(r, list)
                        and len(r) > AMTIDX
                        and str(r[RIDX]).strip() != "SAFE_REMAINDER"
                        and float(r[AMTIDX] or 0.0) > 0.0
                    ]

                    # 送金する行が無いなら、ファイル送信もしない（事故防止）
                    if not transfer_rows:
                        send_telegram(
                            "⚠️ Payout CSV skipped (no transferable rows)\n"
                            f"- week_end: {h(period_end.strftime('%Y-%m-%d %H:%M'))} JST\n"
                            f"- confirmed(FIX): {h(fmt_money(week_claimed))}\n"
                            f"- payout_pct_sum: {h(pct_sum)}%\n"
                            f"- remain_in_safe: {h(fmt_money(remain))}",
                            chat_id=chat_id,
                        )
                        continue

                    csv_path = write_csv(transfer_rows, f"/tmp/{csv_name}")
                    print(f"DBG PAYOUT CSV: {csv_path} pct_sum={pct_sum} remain={remain}", flush=True)

                    caption = (
                        f"📦 {safe_name} payout\n"
                        "✅ Payout prepared\n"
                        f"- week_end: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
                        f"- confirmed(FIX): {fmt_money(week_claimed)}\n"
                        f"- payout_pct_sum: {pct_sum:.1f}%\n"
                        f"- remain_in_safe: {fmt_money(remain)}\n"
                        f"- recipients(active): {len(recipients)}"
                    )

                    # 集約グループへ（ENV優先）
                    try:
                        if csv_hub_chat_id:
                            send_telegram_file(csv_path, chat_id=csv_hub_chat_id, caption=caption)
                            print(f"DBG HUB CSV SENT: {safe_name}", flush=True)
                    except Exception as e:
                        print(f"DBG HUB CSV FAILED: {e}", flush=True)

                    # 各SAFEグループへ
                    send_telegram_file(csv_path, chat_id=chat_id, caption=caption)

                    # Optional: write to WEEKLY_PAYOUTS sheet
                    if _env("PAYOUTS_TO_SHEET", "0") == "1":
                        ws_payouts = get_weekly_payouts_ws(sh)

                        # ✅ シート記録は「0円除外」(remainderは残すのOK)
                        sheet_rows = [
                            r for r in payout_rows
                            if isinstance(r, list)
                            and len(r) > AMTIDX
                            and float(r[AMTIDX] or 0.0) > 0.0
                        ]
                        append_weekly_payout_rows_once(ws_payouts, sheet_rows, period_end, safe_address)

                    # 確認メッセージ（テキスト）
                    send_telegram(
                        "✅ Payout prepared\n"
                        f"- week_end: {h(period_end.strftime('%Y-%m-%d %H:%M'))} JST\n"
                        f"- csv: {h(csv_name)} (attached)\n"
                        f"- confirmed(FIX): {h(fmt_money(week_claimed))}\n"
                        f"- payout_pct_sum: {h(pct_sum)}%\n"
                        f"- remain_in_safe: {h(fmt_money(remain))}\n"
                        f"- recipients(active): {h(len(recipients))}",
                        chat_id=chat_id,
                    )

            else:
                (
                    pos_open,
                    net_total,
                    _uncol_now_value,
                    avg_confirmed_7d_usd,
                    mtd_confirmed_usd,
                    mtd_weth,
                    mtd_usdc,
                ) = compute_daily_revert_metrics(safe_address, period_end)

                msg = build_daily_message(
                    safe_address=safe_address,
                    period_end=period_end,
                    net_total=net_total,
                    pos_open=pos_open,
                    avg_confirmed_7d_usd=avg_confirmed_7d_usd,
                    mtd_confirmed_usd=mtd_confirmed_usd,
                    mtd_weth=mtd_weth,
                    mtd_usdc=mtd_usdc,
                )
                send_telegram(msg, chat_id=chat_id)

        except Exception as e:
            err = (
                "CBC LM ERROR\n\n"
                f"NAME\n{h(safe_name)}\n\n"
                f"SAFE\n{h(safe_address)}\n\n"
                f"ERROR\n{h(type(e).__name__)}: {h(e)}"
            )
            print(err, flush=True)
            try:
                send_telegram(err, chat_id=chat_id)
            except Exception:
                pass
            time.sleep(1)

if __name__ == "__main__":
    main()
