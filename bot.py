import os
import json
import html
import time
import requests
print("DBG BOOT MARKER: 2026-02-24-AAAA", flush=True)
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# ================================
# Cash flow helpers
# ================================
def _norm_cf_type(t) -> str:
    s = str(t or "").strip().lower()
    s = s.replace("_", "-").replace(" ", "-")
    return s


def _is_claimed_type(cf_type) -> bool:
    """
    confirmed fee types (Revert cash_flows) - settlement basis
    """
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

WETH_ADDR = "0x4200000000000000000000000000000000000006".lower()
USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".lower()

def _get_cf_token_addr(cf: dict, which: str) -> str:
    v = cf.get(which)
    if isinstance(v, dict):
        return str(v.get("address") or v.get("id") or "").strip().lower()
    if isinstance(v, str):
        return v.strip().lower()
    return ""

def _get_cf_amount(cf: dict, which: str) -> float:
    if which == "amount0":
        return float(to_f(cf.get("collected_fees_token0") or cf.get("amount0") or 0.0, 0.0) or 0.0)
    else:
        return float(to_f(cf.get("collected_fees_token1") or cf.get("amount1") or 0.0, 0.0) or 0.0)

def _iter_all_cash_flows(pos_all: list[dict]):
    for pos in (pos_all or []):
        for cf in (pos.get("cash_flows") or []):
            if isinstance(cf, dict):
                yield cf

def sum_confirmed_tokens_in_window(pos_all: list[dict], start_dt, end_dt):
    """
    confirmed (claimed-fees / fees-collected) を USD/WETH/USDC で合算
    窓は [start_dt, end_dt) / Noneは無制限
    """
    start_ts = int(start_dt.timestamp()) if start_dt else None
    end_ts = int(end_dt.timestamp()) if end_dt else None

    usd_total = 0.0
    weth_total = 0.0
    usdc_total = 0.0

    for cf in _iter_all_cash_flows(pos_all):
        if not _is_claimed_type(cf.get("type")):
            continue

        ts = _to_ts_sec(cf.get("ts") or cf.get("timestamp") or cf.get("time") or cf.get("created_at"))
        if ts is None:
            continue
        if start_ts is not None and ts < start_ts:
            continue
        if end_ts is not None and ts >= end_ts:
            continue

        usd_total += float(_get_cf_usd(cf) or 0.0)

        t0 = _get_cf_token_addr(cf, "token0")
        t1 = _get_cf_token_addr(cf, "token1")
        a0 = _get_cf_amount(cf, "amount0")
        a1 = _get_cf_amount(cf, "amount1")

        if t0 == WETH_ADDR:
            weth_total += a0
        elif t0 == USDC_ADDR:
            usdc_total += a0

        if t1 == WETH_ADDR:
            weth_total += a1
        elif t1 == USDC_ADDR:
            usdc_total += a1

    return float(usd_total), float(weth_total), float(usdc_total)


def _get_cf_usd(cf: dict) -> float:
    # 既存：usd / usd_value / value_usd / amount_usd ... を拾う処理
    # （ここはそのまま）

    def _f(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    # ここから追加：prices × amount でUSDを復元する（claimed-fees対策）
    prices = cf.get("prices") or {}
    p0 = _f(((prices.get("token0") or {}).get("usd")), 0.0)
    p1 = _f(((prices.get("token1") or {}).get("usd")), 0.0)

    # amount0/1 が無い形もあるので collected_fees_token0/1 も見る
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

    return 0.0
    amt0 = _to_float(cf.get("collected_fees_token0") or cf.get("amount0") or 0.0)
    amt1 = _to_float(cf.get("collected_fees_token1") or cf.get("amount1") or 0.0)

    usd = amt0 * usd0 + amt1 * usd1
    return float(usd)

def _cf_dt_jst(cf: dict) -> Optional[datetime]:
    ts_sec = _to_ts_sec(cf.get("timestamp") or cf.get("ts") or cf.get("time"))
    if not ts_sec:
        return None
    return datetime.fromtimestamp(int(ts_sec), tz=JST)

def pick_confirmed_cf(cash_flows, period_start: datetime, period_end: datetime) -> List[dict]:
    """
    confirmed = fees-collected / claimed-fees
    同一txで両方ある場合は二重計上しない（claimed-fees優先）
    窓は [period_start, period_end)
    返却row:
      usd, amount_weth, amount_usdc, type, tx_hash, nft_id, raw
    """
    rows: List[dict] = []

    def _to_f(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    for cf in (cash_flows or []):
        if not isinstance(cf, dict):
            continue

        t_norm = _norm_cf_type(cf.get("type"))
        if not is_claimed_type(t_norm):
            continue

        dt = _cf_dt_jst(cf)
        if not dt:
            continue
        if not (period_start <= dt < period_end):
            continue

        txh = (_get_tx_hash(cf) or "").lower()
        nft = str(cf.get("nft_id") or cf.get("token_id") or cf.get("_pos_nft_id") or "").strip()
        prices = cf.get("prices") or {}
        p0 = _to_f(((prices.get("token0") or {}).get("usd")))
        p1 = _to_f(((prices.get("token1") or {}).get("usd")))

        # Revertは amount0/1 が文字列のことがある
        a0 = cf.get("amount0")
        a1 = cf.get("amount1")
        a0 = _to_f(a0 if a0 is not None else cf.get("collected_fees_token0"))
        a1 = _to_f(a1 if a1 is not None else cf.get("collected_fees_token1"))

        # USD（まず cf.usd を見て、ダメなら prices×amount）
        usd = _to_f(cf.get("usd"))
        if usd <= 0:
            if p0 > 0 or p1 > 0:
                usd = (a0 * p0) + (a1 * p1)
        if usd < 0:
            usd = 0.0

        # WETH/USDC 推定（USDCはだいたい1.0、WETHはだいたい100以上）
        weth_amt = 0.0
        usdc_amt = 0.0
        if p0 > 100 and 0.9 <= p1 <= 1.1:
            # token0=WETH, token1=USDC
            weth_amt = a0
            usdc_amt = a1
        elif p1 > 100 and 0.9 <= p0 <= 1.1:
            # token1=WETH, token0=USDC
            weth_amt = a1
            usdc_amt = a0
        else:
            if p0 >= p1:
                weth_amt = a0
                usdc_amt = a1
            else:
                weth_amt = a1
                usdc_amt = a0

        # 期間情報（関数冒頭に1回）
dbg("DBG pick_confirmed_cf window JST:", period_start, period_end)

passed = 0
for cf in cash_flows_all:
    # typeフィルタ
    if not _is_claimed_type(cf.get("type")):
        continue

    dt = _cf_dt_jst(cf)
    if not dt:
        continue

    # ★ここが窓内判定
    if not (period_start <= dt < period_end):
        continue
    # ここから窓内PASS
    passed += 1
    
    txh = (_get_tx_hash(cf) or "").lower().strip()
    nft = _get_nft_id(cf)  # 既にあなた側にあるならそのまま。無ければ cf.get("nft_id") 等に置換
    usd = _cf_usd(cf)      # 既にあなた側にあるならそのまま。無ければ既存の usd 算出変数に置換
    weth_amt, usdc_amt = _cf_amounts_weth_usdc(cf)  # 既存の計算結果があるならそれを使う
    
    # 窓内PASS（サマリ用）※上位5件だけ
    if passed <= 5:
        dbg(
            "DBG PASS sample",
            "dt=", dt,
            "type=", cf.get("type"),
            "tx=", (txh[:10] if txh else ""),
            "nft=", nft,
            "usd=", usd,
            "weth=", weth_amt,
            "usdc=", usdc_amt,
        )
        
    rows.append({
        "usd": float(usd or 0.0),
        "amount_weth": float(weth_amt or 0.0),
        "amount_usdc": float(usdc_amt or 0.0),
        "type": cf.get("type") or "",
        "tx_hash": txh,
        "nft_id": nft,
        "raw": cf,
    })
    # 重複排除（claimed優先）
    grouped: Dict[tuple, List[dict]] = {}
    
        for r in rows:
            tx = r.get("tx_hash", "") or ""
            nft = r.get("nft_id", "") or ""
            fallback = ""
            raw = r.get("raw") or {}
            fallback = str(raw.get("timestamp") or raw.get("date") or "")
            k = (tx, nft if nft else f"__no_nft__{fallback}")
            grouped.setdefault(k, []).append(r)
    
        picked: List[dict] = []
        for _, arr in grouped.items():
            if not arr:
                continue
    
            claimed = []
            for item in arr:
                if _norm_cf_type(item.get("type")) == "claimed-fees":
                    claimed.append(item)
    
            target = claimed if claimed else arr
            if not target:
                continue
    
            best = max(target, key=lambda item: float(item.get("usd") or 0.0))
            picked.append(best)
    dbg("DBG pick_confirmed_cf passed/rows:", passed, len(rows))
    dbg("DBG pick_confirmed_cf grouped/picked:", len(grouped), len(picked))
        return picked
_pick_confirmed_cf = pick_confirmed_cf

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

def _sum_weth_usdc_from_rows(rows: list) -> tuple[float, float]:
    weth = 0.0
    usdc = 0.0
    for r in (rows or []):
        raw = r.get("raw") or {}
        t0 = (raw.get("token0_addr") or raw.get("token0") or "").lower()
        t1 = (raw.get("token1_addr") or raw.get("token1") or "").lower()
        a0 = float(raw.get("amount0") or 0.0)
        a1 = float(raw.get("amount1") or 0.0)

        if t0 == WETH_ADDR: weth += a0
        if t1 == WETH_ADDR: weth += a1
        if t0 == USDC_ADDR: usdc += a0
        if t1 == USDC_ADDR: usdc += a1
    return weth, usdc


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


def dbg(*args):
    if (os.getenv("DEBUG") or "").strip() == "1":
        print(*args, flush=True)


def _to_ts_sec(ts):
    """
    Accept epoch seconds/ms, numeric string, or ISO string.
    Returns int seconds (UTC-based epoch).
    """
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


def _get_tx_hash(cf: dict) -> str:
    return str(
        cf.get("tx_hash")
        or cf.get("txHash")
        or cf.get("transaction_hash")
        or cf.get("transactionHash")
        or ""
    ).strip()

def month_start_09_jst(period_end: datetime) -> datetime:
    pe = period_end.astimezone(JST)
    return datetime(pe.year, pe.month, 1, 9, 0, tzinfo=JST)


# ================================
# Time (JST 09:00 anchor)
# ================================
def get_period_end_jst(now: Optional[datetime] = None) -> datetime:
    """
    Daily end aligned to 09:00 JST.
    """
    if now is None:
        now = datetime.now(JST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=JST)
        else:
            now = now.astimezone(JST)

    anchor = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=1)
    return anchor


def get_weekly_period_end_jst(now: Optional[datetime] = None) -> datetime:
    """
    Weekly end aligned to Sunday 09:00 JST (= Sunday 08:00 PH).
    Returns most recent Sunday 09:00 JST (inclusive boundary).
    """
    if now is None:
        now = datetime.now(JST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=JST)
        else:
            now = now.astimezone(JST)

    # weekday: Mon=0 ... Sun=6
    days_since_sun = (now.weekday() - 6) % 7
    sun_9 = (now - timedelta(days=days_since_sun)).replace(hour=9, minute=0, second=0, microsecond=0)
    if now < sun_9:
        sun_9 -= timedelta(days=7)
    return sun_9


def pick_mode_auto(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(JST)
    return "WEEKLY" if now.weekday() == 6 else "DAILY"


def get_mode() -> str:
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
    for i, row in enumerate(rows, start=2):
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
    period_key = f"{period_end_jst.strftime('%Y-%m-%d')} {period_end_jst.hour}:{period_end_jst.strftime('%M')}"
    values = sheets_call(ws.get_all_values) or []

    if not values:
        sheets_call(ws.update, range_name="A1", values=[["", 1]])
        sheets_call(ws.update, range_name="A2", values=[["period_end_jst", safe_name]])
        sheets_call(ws.update, range_name="A3", values=[["safe_address", safe_address]])
        sheets_call(ws.update, range_name="A4", values=[[period_key, float(claimed_usd_24h)]])
        print("DBG: initialized DAILY_WIDE", flush=True)
        return

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

    if safe_name not in header_names:
        current_safe_cols = max(0, len(header_names) - 1)
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
        col_idx = header_names.index(safe_name) + 1
        existing = header_addrs[col_idx - 1] if len(header_addrs) >= col_idx else ""
        if not str(existing or "").strip():
            sheets_call(ws.update_cell, 3, col_idx, safe_address)

    col_idx = header_names.index(safe_name) + 1

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
# Revert API (robust normalize)
# ================================
def fetch_positions(safe: str, active: bool = True):
    url = f"{REVERT_API}/v1/positions/uniswapv3/account/{safe}"
    params = {"active": "true" if active else "false", "with-v4": "true"}
    r = requests.get(url, params=params, timeout=30)
    dbg("DBG fetch_positions", "active=", active, "status=", r.status_code, "url=", r.url)

    if r.status_code != 200:
        dbg("DBG body:", r.text[:800])
    r.raise_for_status()

    js = r.json()
    if isinstance(js, dict):
        dbg("DBG resp keys:", list(js.keys())[:30])
        for k in ("positions", "data", "result"):
            v = js.get(k)
            dbg(f"DBG key {k} type:", type(v).__name__, "len=", (len(v) if isinstance(v, list) else "n/a"))
    else:
        dbg("DBG resp type:", type(js).__name__, "len=", (len(js) if isinstance(js, list) else "n/a"))
    return js


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


def safe_apr_weighted(pos_open: List[dict]) -> float:
    total_net = 0.0
    weighted = 0.0
    for p in (pos_open or []):
        net = float(to_f(calc_net_usd(p), 0.0) or 0.0)
        if net <= 0:
            continue
        apr = float(get_revert_fee_apr(p) or 0.0)
        total_net += net
        weighted += apr * net
    return (weighted / total_net) if total_net > 0 else 0.0


# ================================
# Confirmed fees aggregation (cash_flows)
# ================================
def _cf_iter(pos_all: List[dict]):
    for pos in (pos_all or []):
        if not isinstance(pos, dict):
            continue
        nft_id = str(pos.get("nft_id") or "UNKNOWN")
        cfs = pos.get("cash_flows") or []
        if not isinstance(cfs, list):
            continue
        for cf in cfs:
            if isinstance(cf, dict):
                yield nft_id, cf


def calc_claimed_usd_in_window(pos_all: List[dict], start_dt: datetime, end_dt: datetime) -> Tuple[float, int]:
    """
    confirmed total in window: sum USD for fees-collected/claimed-fees.
    Window: [start, end)
    """
    total = 0.0
    count = 0
    seen = set()

    for nft_id, cf in _cf_iter(pos_all):
        if not _is_claimed_type(cf.get("type")):
            continue

        ts = _to_ts_sec(cf.get("timestamp"))
        if ts is None:
            continue
        ts_dt = datetime.fromtimestamp(ts, JST)
        if ts_dt < start_dt or ts_dt >= end_dt:
            continue

        if os.getenv("DBG_CF_KEYS", "0") == "1":
            print("DBG CF type:", cf.get("type"), flush=True)
            print("DBG CF keys:", list(cf.keys()), flush=True)
            print("DBG CF sample:", str(cf)[:800], flush=True)
            # 1回だけ出して止める
            os.environ["DBG_CF_KEYS"] = "0"

        txh = _get_tx_hash(cf)
        usd = _get_cf_usd(cf)
        print("DBG ADD", cf.get("date"), cf.get("type"), "usd=", usd, "tx=", _get_tx_hash(cf), "nft=", nft_id, flush=True)
        print("DBG cf.usd_calc:", usd, flush=True)
        if usd is None or usd <= 0:
            continue

        # de-dup (same event can appear multiple times)
        key = (txh, _norm_cf_type(cf.get("type")), nft_id, int(ts), round(float(usd), 6))
        if key in seen:
            continue
        seen.add(key)

        total += float(usd)
        count += 1

    return float(total), int(count)


def calc_claimed_usd_by_nft_in_window(pos_all: List[dict], start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    out: Dict[str, float] = {}
    seen = set()

    for nft_id, cf in _cf_iter(pos_all):
        if not _is_claimed_type(cf.get("type")):
            continue

        ts = _to_ts_sec(cf.get("timestamp"))
        if ts is None:
            continue
        ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(JST)
        if ts_dt < start_dt or ts_dt >= end_dt:
            continue

        usd = _get_cf_usd(cf)
        print("DBG ADD", cf.get("date"), cf.get("type"), "usd=", usd, "tx=", _get_tx_hash(cf), "nft=", nft_id, flush=True)
        if usd is None or usd <= 0:
            continue

        txh = _get_tx_hash(cf)
        key = (txh, _norm_cf_type(cf.get("type")), nft_id)
        if key in seen:
            continue
        seen.add(key)

        out[nft_id] = float(out.get(nft_id, 0.0) or 0.0) + float(usd)

    return out


def calc_confirmed_all_time(pos_all: List[dict]) -> float:
    total = 0.0
    seen = set()
    for nft_id, cf in _cf_iter(pos_all):
        if not _is_claimed_type(cf.get("type")):
            continue
        ts = _to_ts_sec(cf.get("timestamp"))
        if ts is None:
            continue
        usd = _get_cf_usd(cf)
        if usd is None or usd <= 0:
            continue
        txh = _get_tx_hash(cf)
        key = (txh, _norm_cf_type(cf.get("type")), nft_id, int(ts), round(float(usd), 6))
        if key in seen:
            continue
        seen.add(key)
        total += float(usd)
    return float(total)


def calc_confirmed_month_to_date(pos_all: List[dict], period_end: datetime) -> float:
    pe = period_end.astimezone(JST)
    start_month = pe.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    total, _ = calc_claimed_usd_in_window(pos_all, start_month, pe)
    return float(total)


# ================================
# Metrics derived from Sheets log
# (Daily emitted logic uses Sheets; Weekly MTD/ALLTIME uses Revert confirmed)
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


def get_yesterday_unclaimed_from_history(history: List[List[str]]) -> float:
    if not history:
        return 0.0
    if len(history) >= 2:
        return _row_val(history[-2], 5)
    return 0.0


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
            f"{nft_link} | {h(status)} | Net {fmt_money(net)} | APR {fmt_pct(apr)}"
        )
    return lines


# ================================
# Messages (Daily / Weekly)
# ================================
def build_daily_message(
    safe_address: str,
    period_end: datetime,
    net_total: float,
    emitted_today: float,  # ←互換のため残す（この関数内では使わない）
    history: List[List[str]],
    pos_open: List[dict],
) -> str:
    # --------------------------------
    # ① 「現在DEX手数料収益（Value）」＝ uncollected現在値（positionsの現在値合算）
    #    ※ Revert Positions API では fees_value が未回収USD相当として入っていることが多い
    # --------------------------------
    uncollected_now_value = 0.0
    for p in (pos_open or []):
        try:
            # fees_value を優先、無ければ uncollected_fees_value 等へフォールバック
            if p.get("fees_value") is not None:
                uncollected_now_value += float(p.get("fees_value") or 0.0)
            elif p.get("uncollected_fees_value") is not None:
                uncollected_now_value += float(p.get("uncollected_fees_value") or 0.0)
        except Exception:
            pass

    # --------------------------------
    # ② 7日平均 / MTD は「confirmedのみ」で計算
    #    あなたのシート構造（推定）:
    #    [period_end, safe_name, safe_address, net_total, claimed_24h, unclaimed, emitted]
    #                          index:    0         1        2         3         4        5      6
    #    → confirmedは claimed_24h（index=4）を使う
    # --------------------------------
    CONFIRMED_COL = 4

    confirmed_7d = sum_last_n_days(history, 7, CONFIRMED_COL)
    avg_confirmed_7d = confirmed_7d / 7.0 if confirmed_7d > 0 else 0.0

    mtd_confirmed = sum_month_to_date(history, period_end, CONFIRMED_COL)

    # --------------------------------
    # ③ APR（直近7日平均）＝ confirmedのみ（安定版）
    #    APR = (avg_confirmed_7d / net_total) * 365
    # --------------------------------
    apr_7d = (avg_confirmed_7d / net_total) * 365.0 * 100.0 if net_total > 0 else 0.0

    safe_link = fmt_safe_link(safe_address)

    msg = (
        "🚀 CBC Liquidity Mining — Daily\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"SAFE {safe_link}\n"
        "────────────────\n\n"
        "🗓 現在DEX手数料収益（Value）\n"
        f"{fmt_money(uncollected_now_value)}\n\n"
        "📈 推定戦略APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "────────────────\n"
        "🎉 月間累計 確定DEX手数料収益\n"
        f"{fmt_money(mtd_confirmed)}（Value）\n\n"
        "📆 1日あたり確定DEX手数料収益（直近7日平均）\n"
        f"{fmt_money(avg_confirmed_7d)}\n"
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
    # 追加：枚数（無ければ 0.0 を渡せばOK）
    week_weth: float = 0.0,
    week_usdc: float = 0.0,
    mtd_weth: float = 0.0,
    mtd_usdc: float = 0.0,
    all_weth: float = 0.0,
    all_usdc: float = 0.0,
    pos_open: List[dict] = None,
) -> str:
    pos_open = pos_open or []

    avg_claimed_day = week_claimed / 7.0 if week_claimed > 0 else 0.0

    wow_txt = "—"
    if prev_week_claimed > 0:
        wow = ((week_claimed - prev_week_claimed) / prev_week_claimed) * 100.0
        sign = "+" if wow >= 0 else ""
        wow_txt = f"{sign}{wow:.1f}%"

    safe_link = fmt_safe_link(safe_address)

    # APRは confirmed-only に寄せる（安定）
    apr_7d = (avg_claimed_day / net_total) * 365.0 * 100.0 if net_total > 0 else 0.0

    # NFT linesはそのまま（見やすいなら残す）
    nft_lines = build_nft_lines_revert_apr(pos_open)

    msg = (
        "🚀 CBC Liquidity Mining — Weekly Settlement\n"
        f"Period End: {period_end.strftime('%Y-%m-%d %H:%M')} JST\n"
        f"SAFE {safe_link}\n"
        "────────────────\n\n"
        "🎉 今週 確定DEX手数料収益（FIX）\n"
        f"{week_weth:.6f} ETH\n"
        f"{week_usdc:,.2f} USDC\n"
        f"{fmt_money(week_claimed)}\n"
        f"（前週 {fmt_money(prev_week_claimed)} ／ {wow_txt}）\n\n"
        "📆 1日あたり平均確定手数料収益\n"
        f"{fmt_money(avg_claimed_day)}\n\n"
        "🔒 現在Net運用額\n"
        f"{fmt_money(net_total)}\n\n"
        "────────────────\n"
        "📈 推定戦略 APR（直近7日平均）\n"
        f"{fmt_pct(apr_7d)}\n\n"
        "🗓 今月累計 確定DEX手数料収益\n"
        f"{mtd_weth:.6f} ETH  {mtd_usdc:,.2f} USDC\n"
        f"{fmt_money(mtd_confirmed)}\n\n"
        "🏆 ALL-TIME 確定手数料収益\n"
        f"{all_weth:.6f} ETH  {all_usdc:,.2f} USDC\n"
        f"{fmt_money(all_confirmed)}\n\n"
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
# Core compute
# ================================
def compute_today_metrics(
    safe_address: str,
    period_end: datetime,
) -> Tuple[List[dict], List[dict], float, float, float]:
    """
    Returns:
      pos_open, pos_all, net_total_usd, claimed_24h_usd, unclaimed_usd
    """
    start_dt = period_end - timedelta(days=1)

    resp_open = fetch_positions(safe_address, active=True)
    resp_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(resp_open)
    pos_exited = _normalize_positions(resp_exited)
    pos_all = pos_open + pos_exited

    dbg("DBG today normalize open/exited lens:", len(pos_open), len(pos_exited))
    if len(pos_open) == 0:
        dbg("DBG today open raw (first 800):", str(resp_open)[:800])

    net_total = 0.0
    unclaimed = 0.0
    for pos in (pos_open or []):
        net = calc_net_usd(pos)
        net_total += float(net) if net is not None else 0.0
        unclaimed += float(to_f(pos.get("fees_value"), 0.0) or 0.0)

    claimed_24h, _tx_24h = calc_claimed_usd_in_window(pos_all, start_dt, period_end)

    return pos_open, pos_all, float(net_total), float(claimed_24h), float(unclaimed)


def compute_weekly_confirmed_metrics(
    safe_address: str,
    period_end: datetime,
) -> Tuple[List[dict], float, Dict[str, float], float, float, float, float]:
    """
    Returns:
      pos_open, net_total, confirmed_by_nft_7d, week_total, prev_week_total, mtd_confirmed, all_time_confirmed
    """
    start_this = period_end - timedelta(days=7)
    end_this = period_end
    start_prev = period_end - timedelta(days=14)
    end_prev = period_end - timedelta(days=7)

    resp_open = fetch_positions(safe_address, active=True)
    resp_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(resp_open)
    pos_exited = _normalize_positions(resp_exited)
    pos_all = pos_open + pos_exited
        # --- cash_flows を安全に集める（必ず定義しておく） ---
    cash_flows_all: List[dict] = []
    for pos in (pos_all or []):
        cfs = pos.get("cash_flows") or []
        if isinstance(cfs, list):
            cash_flows_all.extend(cfs)

    net_total = 0.0
    for pos in pos_open:
        net_total += float(to_f(calc_net_usd(pos), 0.0) or 0.0)

    confirmed_by_nft_7d = calc_claimed_usd_by_nft_in_window(pos_all, start_this, end_this)
    confirmed_by_nft_prev = calc_claimed_usd_by_nft_in_window(pos_all, start_prev, end_prev)

    week_total = float(sum((confirmed_by_nft_7d or {}).values()))
    prev_week_total = float(sum((confirmed_by_nft_prev or {}).values()))
    
    # =========================
    # ここから追加
    # =========================
    dbg("DBG cash_flows_all len", len(cash_flows_all))
    dbg("DBG WEEK WINDOW start/end JST:", start_this, end_this)
    week_rows = pick_confirmed_cf(cash_flows_all, start_this, end_this)
    week_weth = sum(r.get("amount_weth", 0.0) for r in week_rows)
    week_usdc = sum(r.get("amount_usdc", 0.0) for r in week_rows)
    week_total = sum(r.get("usd", 0.0) for r in week_rows)
    print("DBG WEEK SUM",
      "weth=", week_weth,
      "usdc=", week_usdc,
      "usd=", week_total,
      flush=True)
    
    dbg("DBG week_rows len", len(week_rows))
    if week_rows:
        dbg("DBG week_row keys", list(week_rows[0].keys()))
        dbg("DBG week_row sample", str(week_rows[0])[:1200])

# =========================
# 追加ここまで
# =========================
    
    # --- MTD / ALL confirmed (period_end基準で統一) ---
    month_start = datetime(
        period_end.astimezone(JST).year,
        period_end.astimezone(JST).month,
        1, 0, 0,
        tzinfo=JST
    )
    
    # pos_all から cash_flows を集める
    cash_flows_all = []
    for pos in (pos_all or []):
        pos_nft = str(pos.get("nft_id") or "").strip()
        cfs = pos.get("cash_flows") or []
        if not isinstance(cfs, list):
            continue
        for cf in cfs:
            if isinstance(cf, dict) and pos_nft:
                # cf側にnft_idが無い/空なら pos由来を補完
                if not (cf.get("nft_id") or cf.get("token_id")):
                    cf["_pos_nft_id"] = pos_nft
            cash_flows_all.append(cf)
    # --- MTD ---
    mtd_rows = pick_confirmed_cf(cash_flows_all, month_start, period_end)
    
    mtd_confirmed = float(sum((r.get("usd") or 0.0) for r in mtd_rows))
    mtd_weth = float(sum((r.get("amount_weth") or 0.0) for r in mtd_rows))
    mtd_usdc = float(sum((r.get("amount_usdc") or 0.0) for r in mtd_rows))
    # --- ALL ---
    all_start = datetime(2020, 1, 1, tzinfo=JST)
    all_rows = pick_confirmed_cf(cash_flows_all, all_start, period_end)
    
    all_confirmed = float(sum((r.get("usd") or 0.0) for r in all_rows))
    all_weth = float(sum((r.get("amount_weth") or 0.0) for r in all_rows))
    all_usdc = float(sum((r.get("amount_usdc") or 0.0) for r in all_rows))
    dbg("DBG weekly confirmed this/prev:", week_total, prev_week_total)
    dbg("DBG weekly mtd/all:", mtd_confirmed, all_confirmed)

    return (
        pos_open,
        float(net_total),
        confirmed_by_nft_7d,
        week_total,
        prev_week_total,
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

    # period_end
    if mode == "WEEKLY":
        period_end = get_weekly_period_end_jst()
    else:
        period_end = get_period_end_jst()

    print("DBG period_end JST:", period_end.strftime("%Y-%m-%d %H:%M"), flush=True)

    # Sheets init（Daily history用）
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
            safe_hist = get_safe_history(all_rows, safe_address)

            # ================================
            # WEEKLY
            # ================================
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
                send_telegram(msg, chat_id)
                continue

            # ================================
            # DAILY
            # ================================
            pos_open, _pos_all, net_total, claimed_24h, unclaimed_today = compute_today_metrics(
                safe_address, period_end
            )

            y_unclaimed = get_yesterday_unclaimed_from_history(safe_hist)
            delta_unclaimed = unclaimed_today - y_unclaimed
            if delta_unclaimed < 0:
                delta_unclaimed = 0.0
            emitted_today = float(delta_unclaimed) + float(claimed_24h)

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

            if sheets_enabled and ws_wide:
                append_daily_wide_numbered(ws_wide, period_end, safe_name, safe_address, claimed_24h)

            msg = build_daily_message(
                safe_address=safe_address,
                period_end=period_end,
                net_total=net_total,
                emitted_today=emitted_today,
                history=safe_hist,
                pos_open=pos_open,
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
