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
# Constants (JST MUST be defined early)
# ================================
JST = timezone(timedelta(hours=9))
REVERT_API = "https://api.revert.finance"

WETH_ADDR = "0x4200000000000000000000000000000000000006".lower()
USDC_ADDR = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".lower()

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

def dbg(*args):
    if (os.getenv("DEBUG") or "").strip() == "1":
        print(*args, flush=True)

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

# ================================
# Cash flow helpers
# ================================
def _norm_cf_type(t) -> str:
    s = str(t or "").strip().lower()
    s = s.replace("_", "-").replace(" ", "-")
    return s

# ✅ FIX: global alias (prevents "name 'norm_cf_type' is not defined")
norm_cf_type = _norm_cf_type

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

def _iter_all_cash_flows(pos_all: List[dict]):
    for pos in (pos_all or []):
        for cf in (pos.get("cash_flows") or []):
            if isinstance(cf, dict):
                yield cf

def ts_to_dt(ts):
    if ts is None:
        return None

    # numeric (sec or ms)
    if isinstance(ts, (int, float)):
        x = float(ts)
        if x > 1e12:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=JST)

    # string ISO
    if isinstance(ts, str):
        s = ts.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).astimezone(JST)
        except Exception:
            return None

    return None

def _get_cf_usd(cf: dict) -> float:
    """
    prices × amount でUSDを復元する（claimed-fees対策）
    """
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

    # fallback: try direct fields if ever present
    for k in ("usd", "usd_value", "value_usd", "amount_usd", "valueUsd", "amountUsd"):
        v = cf.get(k)
        vv = _f(v, 0.0)
        if vv > 0:
            return vv

    return 0.0

def sum_confirmed_tokens_in_window(pos_all: List[dict], start_dt, end_dt):
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

def pick_confirmed_cf(cash_flows: List[dict], period_start: datetime, period_end: datetime) -> List[dict]:
    """
    cash_flows から confirmed-only を拾って、窓 [period_start, period_end) に入るものを
    USD / amount_weth / amount_usdc 推定付きで rows 化して返す（重複排除あり）
    """
    types = {}
    print("DBG cf sample keys:", list(cash_flows[0].keys()) if cash_flows else [], flush=True)

    for r in cash_flows:
        t = str(r.get("type") or r.get("cash_flow_type") or r.get("event_type") or "").strip()
        types[t] = types.get(t, 0) + 1

    print("DBG cf types top:", sorted(types.items(), key=lambda x: -x[1])[:10], flush=True)

    def _to_f(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    dbg("DBG pick_confirmed_cf window JST:", period_start, period_end)

    rows: List[dict] = []
    passed = 0
    shown = 0

    for cf in (cash_flows or []):
        if not isinstance(cf, dict):
            continue

        t_norm = _norm_cf_type(cf.get("type"))
        if not _is_claimed_type(t_norm):
            continue

        # timestamp key candidates
        dt = (
            ts_to_dt(cf.get("timestamp"))
            or ts_to_dt(cf.get("ts"))
            or ts_to_dt(cf.get("time"))
            or ts_to_dt(cf.get("created_at"))
            or ts_to_dt(cf.get("date"))
        )

        if t_norm == "claimed-fees" and shown < 5:
            print(
                "DBG claimed-fees dt check:",
                "raw_ts=", cf.get("timestamp"),
                "dt=", dt,
                "type=", cf.get("type"),
                flush=True
            )
            shown += 1

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

def get_weekly_log_ws(sh):
    """
    WEEKLY_LOG シートを取得
    環境変数 GOOGLE_SHEET_WEEKLY_LOG_TAB があればそれを使用
    無ければ 'WEEKLY_LOG'
    """
    tab_name = os.getenv("GOOGLE_SHEET_WEEKLY_LOG_TAB", "WEEKLY_LOG")
    return sh.worksheet(tab_name)

def append_weekly_log_row_once(
    ws,
    week_ending,
    safe_name,
    safe_address,
    confirmed_weth,
    confirmed_usdc,
    confirmed_usd_fix,
):
    """
    WEEKLY_LOG に 週×SAFE で1行だけ追加
    既に同じ週＋SAFEがあればスキップ
    """

    week_key = week_ending.strftime("%Y-%m-%d %H:%M")

    # 既存行チェック
    existing = ws.get_all_values()
    for row in existing[1:]:  # ヘッダー除外
        if len(row) < 3:
            continue
        if row[0] == week_key and row[2] == safe_address:
            print(f"DBG: WEEKLY_LOG skip existing {safe_name} {week_key}")
            return

    # 新規追加
    ws.append_row([
        week_key,
        safe_name,
        safe_address,
        float(confirmed_weth),
        float(confirmed_usdc),
        float(confirmed_usd_fix),
    ], value_input_option="USER_ENTERED")

    print(f"DBG: WEEKLY_LOG appended {safe_name} {week_key}")

def get_config_recipients_ws(sh):
    tab_name = os.getenv("GOOGLE_SHEET_CONFIG_TAB", "CONFIG_RECIPIENTS")
    return sh.worksheet(tab_name)

def load_active_recipients_for_safe(sh, safe_name: str):
    ws = get_config_recipients_ws(sh)
    rows = sheets_call(ws.get_all_records) or []

    result = []
    for r in rows:
        if str(r.get("safe_name", "")).strip() != safe_name:
            continue
        if not r.get("active"):
            continue

        pct_raw = str(r.get("pct", "")).replace("%", "").strip()
        try:
            pct = float(pct_raw) / 100.0
        except Exception:
            pct = 0.0

        result.append({
            "recipient_id": r.get("recipient_id"),
            "name": r.get("name"),
            "address": r.get("address"),
            "pct": pct,
        })

    return result

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

# ================================
# Time (JST 00:00 close; send at 09:00)
# ================================
def get_period_end_jst(now: Optional[datetime] = None) -> datetime:
    """
    Daily end aligned to 00:00 JST.
    """
    if now is None:
        now = datetime.now(JST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=JST)
        else:
            now = now.astimezone(JST)

    anchor = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=1)
    return anchor

def get_weekly_period_end_jst(now: Optional[datetime] = None) -> datetime:
    """
    Weekly end aligned to Sunday 00:00 JST.
    Returns most recent Sunday 00:00 JST.
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
    sun_0 = (now - timedelta(days=days_since_sun)).replace(hour=0, minute=0, second=0, microsecond=0)
    if now < sun_0:
        sun_0 -= timedelta(days=7)
    return sun_0

def pick_mode_auto(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(JST)
    return "WEEKLY" if now.weekday() == 6 else "DAILY"

def get_mode() -> str:
    raw = (os.getenv("REPORT_MODE") or "").strip().upper()
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
    # 現在DEX手数料（Value）＝ uncollected now
    uncollected_now_value = 0.0
    for p in (pos_open or []):
        try:
            if p.get("fees_value") is not None:
                uncollected_now_value += float(p.get("fees_value") or 0.0)
            elif p.get("uncollected_fees_value") is not None:
                uncollected_now_value += float(p.get("uncollected_fees_value") or 0.0)
        except Exception:
            pass

    # APR（直近7日平均）＝ confirmed-only
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

    # APR confirmed-only
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
    path = os.environ.get("CONFIG_PATH", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ================================
# Daily compute (Revert-only)
# ================================
def compute_daily_revert_metrics(
    safe_address: str,
    period_end: datetime,
) -> Tuple[List[dict], float, float, float, float, float, float]:
    """
    Daily (LIVE, REVERT-only)
    Returns:
      pos_open,
      net_total,
      uncollected_now_value,
      avg_confirmed_7d_usd,
      mtd_confirmed_usd,
      mtd_weth,
      mtd_usdc
    """
    resp_open = fetch_positions(safe_address, active=True)
    resp_exited = fetch_positions(safe_address, active=False)

    pos_open = _normalize_positions(resp_open)
    pos_exited = _normalize_positions(resp_exited)
    pos_all = (pos_open or []) + (pos_exited or [])

    # net_total（openだけ）
    net_total = 0.0
    for pos in (pos_open or []):
        net_total += float(to_f(calc_net_usd(pos), 0.0) or 0.0)

    # uncollected now（openの fees_value 合算）
    uncollected_now_value = 0.0
    for p in (pos_open or []):
        try:
            if p.get("fees_value") is not None:
                uncollected_now_value += float(p.get("fees_value") or 0.0)
            elif p.get("uncollected_fees_value") is not None:
                uncollected_now_value += float(p.get("uncollected_fees_value") or 0.0)
        except Exception:
            pass

    # cash_flows all（nft_id補完つき）
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

    # 7d confirmed window（[end-7d, end)）
    start_7d = period_end - timedelta(days=7)
    rows_7d = pick_confirmed_cf(cash_flows_all, start_7d, period_end)
    confirmed_7d = float(sum((r.get("usd") or 0.0) for r in rows_7d))
    avg_confirmed_7d_usd = confirmed_7d / 7.0 if confirmed_7d > 0 else 0.0

    # MTD confirmed window（[month_start, end)）
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
# Weekly compute (confirmed-only + amounts)
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
    """
    Returns:
      pos_open,
      net_total,
      confirmed_by_nft_7d (legacy map; may be empty),
      week_total_usd, prev_week_total_usd,
      mtd_confirmed_usd, all_confirmed_usd,
      week_weth, week_usdc,
      mtd_weth, mtd_usdc,
      all_weth, all_usdc
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

    # cash_flows all（まず集める）
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

    # net_total open-only
    net_total = 0.0
    for pos in (pos_open or []):
        net_total += float(to_f(calc_net_usd(pos), 0.0) or 0.0)

    # 週合計（USD/WETH/USDC）
    dbg("DBG cash_flows_all len", len(cash_flows_all))
    dbg("DBG WEEK WINDOW start/end JST:", start_this, end_this)

    week_rows = pick_confirmed_cf(cash_flows_all, start_this, end_this)
    week_weth = float(sum(r.get("amount_weth", 0.0) for r in week_rows))
    week_usdc = float(sum(r.get("amount_usdc", 0.0) for r in week_rows))
    week_total = float(sum(r.get("usd", 0.0) for r in week_rows))

    prev_rows = pick_confirmed_cf(cash_flows_all, start_prev, end_prev)
    prev_week_total = float(sum(r.get("usd", 0.0) for r in prev_rows))

    print("DBG WEEK SUM",
          "weth=", week_weth,
          "usdc=", week_usdc,
          "usd=", week_total,
          flush=True)

    # MTD / ALL
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

    dbg("DBG weekly confirmed this/prev:", week_total, prev_week_total)
    dbg("DBG weekly mtd/all:", mtd_confirmed, all_confirmed)

    # legacy map (optional / keep interface)
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

    # period_end（00:00〆）
    if mode == "WEEKLY":
        period_end = get_weekly_period_end_jst()
    else:
        period_end = get_period_end_jst()

    print("DBG period_end JST:", period_end.strftime("%Y-%m-%d %H:%M"), flush=True)

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
            # ================================
            # WEEKLY (FIX)
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

                # ---- payout (WEEKLY only) ----
                client = get_gsheet_client()
                sh = open_sheet(client)
                
                # ✅ WEEKLY_LOG に 1行書く（週×SAFEで重複はスキップ）
                ws_weekly_log = get_weekly_log_ws(sh)
                append_weekly_log_row_once(
                    ws_weekly_log,
                    week_ending=period_end,
                    safe_name=safe_name,
                    safe_address=safe_address,
                    confirmed_weth=week_weth,
                    confirmed_usdc=week_usdc,
                    confirmed_usd_fix=week_claimed,  # USD(FIX)
                )
                
                # (既存) payout sheet
                ws_payouts = sh.worksheet(os.getenv("GOOGLE_SHEET_PAYOUTS_TAB", "WEEKLY_PAYOUTS"))
                recipients = load_active_recipients_for_safe(sh, safe_name)

                total_usdc_base = float(week_claimed)  # 原資(USDC扱い)
                pay_recipients = [
                    r for r in recipients
                    if str(r.get("recipient_id", "")).lower() != "system"
                ]
                sum_pay_pct = sum(float(r.get("pct", 0.0)) for r in pay_recipients)

                week_key = period_end.strftime("%Y-%m-%d %H:%M")
                created_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M")

                for r in pay_recipients:
                    pct = float(r.get("pct", 0.0))
                    pct_norm = (pct / sum_pay_pct) if sum_pay_pct > 0 else 0.0
                    amount = round(total_usdc_base * pct_norm, 6)

                    row = [
                        week_key,
                        safe_name,
                        safe_address,
                        r.get("recipient_id"),
                        r.get("name"),
                        r.get("address"),
                        amount,
                        created_at,
                    ]
                    sheets_call(ws_payouts.append_row, row, value_input_option="USER_ENTERED")

                print(f"DBG before send_telegram mode={mode} name={safe_name} chat_id={chat_id} msg_len={len(msg)}", flush=True)
                send_telegram(msg, chat_id)

                continue  # ✅ weeklyはdailyへ落とさない

            # ================================
            # DAILY (LIVE, REVERT-only)
            # ================================
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
