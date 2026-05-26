#!/usr/bin/env python3
"""
CM Multi Asset Fund — Daily Update
Fetches NAV + returns + AUM for CM Multi Asset and all peer funds
from AMFI (category=1, subCategory=3).
Inception date: 2026-03-16

Usage:
    python multiasset_daily.py
    python multiasset_daily.py --full   (rebuild full history)
"""
import sqlite3, os, sys, datetime, time, requests
import pandas as pd

BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
DB_PATH        = os.path.join(BASE_DIR, "multiasset_rankings.db")
INCEPTION_DATE = datetime.date(2026, 3, 16)
CM_LABEL       = "Capitalmind"

AMFI_URL = "https://www.amfiindia.com/gateway/pollingsebi/api/amfi/fundperformance"
AMFI_NAV_URL  = "https://portal.amfiindia.com/spages/NAVOpen.txt"
AMFI_HIST_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

AMFI_PAYLOAD = {"maturityType": 1, "category": 1, "subCategory": 3, "mfid": 0}
AMFI_COOKIES = {"__gsas": "ID=6b552a2ac1780742:T=1767694252:RT=1767694252:S=ALNI_MZso2q-UuvEjM4NBfG3Nn6YjAReeQ"}
AMFI_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.amfiindia.com",
    "Referer": "https://www.amfiindia.com/polling/amfi/fund-performance",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Scheme codes for direct plans — category 1/subCategory 3 (Multi Asset Allocation)
# Capitalmind Multi Asset is identified by "CAPITALMIND" in scheme name
SCHEME_CODES = {}  # will be populated dynamically from AMFI API

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS nav_rankings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    nav_date   TEXT NOT NULL,
    fund_label TEXT NOT NULL,
    nav        REAL,
    aum_cr     REAL,
    n_1d       INTEGER,
    ret_1d     REAL, ret_7d   REAL, ret_30d  REAL,
    ret_91d    REAL, ret_184d REAL, ret_365d REAL, ret_si REAL,
    rank_1d    INTEGER, rank_7d   INTEGER, rank_30d  INTEGER,
    rank_91d   INTEGER, rank_184d INTEGER, rank_365d INTEGER, rank_si INTEGER,
    UNIQUE(nav_date, fund_label)
);
CREATE TABLE IF NOT EXISTS nav_dates (
    nav_date   TEXT PRIMARY KEY,
    t1_override TEXT
);
CREATE INDEX IF NOT EXISTS idx_nav_date ON nav_rankings(nav_date);
"""

def init_db(conn):
    conn.executescript(DB_SCHEMA)
    conn.commit()


def fetch_amfi_data(report_date_str):
    """Fetch fund performance data from AMFI API for a specific date."""
    payload = dict(AMFI_PAYLOAD)
    payload["reportDate"] = report_date_str
    for attempt in range(1, 4):
        try:
            r = requests.post(AMFI_URL, cookies=AMFI_COOKIES,
                              headers=AMFI_HEADERS, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            items = data if isinstance(data, list) else data.get("data", [])
            return items
        except Exception as e:
            if attempt == 3: return []
            time.sleep(2 * attempt)
    return []


def get_fund_label(scheme_name):
    """Extract AMC name — everything before 'Multi Asset' / 'Multi-Asset' in the scheme name."""
    import re
    if "CAPITALMIND" in scheme_name.upper():
        return "Capitalmind"
    m = re.search(r'\bMulti[\s\-]Asset\b', scheme_name, re.IGNORECASE)
    if m:
        label = scheme_name[:m.start()].strip(" -\u2013\u2014")
        if label:
            return label
    # Fallback: first three words
    return " ".join(scheme_name.split()[:3])


def parse_float(v):
    if v is None: return None
    try: return float(str(v).replace(",", ""))
    except: return None


def ingest(full_history=False, override_t1=None):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    today = datetime.date.today()
    today_str = today.isoformat()

    # Find T-1 date
    def get_t1(for_date):
        if override_t1 and for_date in override_t1:
            return override_t1[for_date]
        existing = [r["nav_date"] for r in conn.execute(
            "SELECT DISTINCT nav_date FROM nav_dates WHERE nav_date < ? ORDER BY nav_date DESC LIMIT 1",
            (for_date,)
        ).fetchall()]
        return existing[0] if existing else None

    print(f"\n{'='*50}")
    print(f"  CM MULTI ASSET — {'Full Rebuild' if full_history else 'Daily Update'}")
    print(f"{'='*50}")

    # Fetch today's data from AMFI
    print(f"Fetching AMFI data for {today_str}...")
    amfi_str = today.strftime("%d-%b-%Y")
    items = fetch_amfi_data(amfi_str)

    # Try previous days if no data
    if not items:
        for delta in range(1, 8):
            d = today - datetime.timedelta(days=delta)
            amfi_str = d.strftime("%d-%b-%Y")
            print(f"  {amfi_str}: no data, trying previous day...")
            items = fetch_amfi_data(amfi_str)
            if items:
                today_str = d.isoformat()
                today = d
                break

    if not items:
        print("  ERROR: No data from AMFI"); conn.close(); return

    df = pd.DataFrame(items)
    print(f"  {amfi_str}: {len(df)} funds loaded")

    # Get AUM column
    aum_col = "dailyAUM" if "dailyAUM" in df.columns else None

    # Build fund list
    funds_data = []
    for _, row in df.iterrows():
        name = str(row.get("schemeName", "")).strip()
        if not name: continue
        # Only direct plans
        if "DIRECT" not in name.upper(): continue
        # Only growth option
        if any(x in name.upper() for x in ["DIVIDEND", "IDCW", "BONUS"]): continue

        label = get_fund_label(name)
        nav_val = parse_float(row.get("navDirect") or row.get("navRegular"))
        aum_val = parse_float(row.get(aum_col)) if aum_col else None

        # Returns — de-annualise AMFI figures to absolute: val * n_days / 365
        def deannualise(v, n): return round(v * n / 365, 6) if v is not None else None
        # 1D: use preNavDirect if available, else de-annualise 7D
        pre_nav = parse_float(row.get("preNavDirect"))
        if nav_val and pre_nav and pre_nav > 0:
            ret_1d = round((nav_val / pre_nav - 1) * 100, 6)
        else:
            v7 = parse_float(row.get("return7DaysDirect"))
            ret_1d = deannualise(v7, 7)
        funds_data.append({
            "fund_label": label,
            "nav":        nav_val,
            "aum_cr":     aum_val,
            "ret_1d":     ret_1d,
            "ret_7d":     deannualise(parse_float(row.get("return7DaysDirect")),   7),
            "ret_30d":    deannualise(parse_float(row.get("return1MonthDirect")),  30),
            "ret_91d":    deannualise(parse_float(row.get("return3MonthDirect")),  91),
            "ret_184d":   deannualise(parse_float(row.get("return6MonthDirect")), 184),
            "ret_365d":   deannualise(parse_float(row.get("return1YearDirect")),  365),
            "ret_si":     parse_float(row.get("returnSinceLaunchDirect")),  # AMFI SI is already absolute
        })

    if not funds_data:
        print("  No direct plan funds found"); conn.close(); return

    n_funds = len(funds_data)
    print(f"  {n_funds} direct plan funds found")

    # Calculate ranks
    for period in ["1d","7d","30d","91d","184d","365d","si"]:
        key = f"ret_{period}"
        ranked = sorted([f for f in funds_data if f[key] is not None],
                        key=lambda x: x[key], reverse=True)
        for rank, f in enumerate(ranked, 1):
            f[f"rank_{period}"] = rank
        for f in funds_data:
            if f.get(f"rank_{period}") is None:
                f[f"rank_{period}"] = None

    # Save to DB
    nav_count = aum_count = 0
    for f in funds_data:
        conn.execute("""INSERT OR REPLACE INTO nav_rankings
            (nav_date, fund_label, nav, aum_cr, n_1d,
             ret_1d, ret_7d, ret_30d, ret_91d, ret_184d, ret_365d, ret_si,
             rank_1d, rank_7d, rank_30d, rank_91d, rank_184d, rank_365d, rank_si)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (today_str, f["fund_label"], f["nav"], f["aum_cr"], n_funds,
             f["ret_1d"], f["ret_7d"], f["ret_30d"], f["ret_91d"],
             f["ret_184d"], f["ret_365d"], f["ret_si"],
             f["rank_1d"], f["rank_7d"], f["rank_30d"], f["rank_91d"],
             f["rank_184d"], f["rank_365d"], f["rank_si"]))
        nav_count += 1
        if f["aum_cr"]: aum_count += 1

    conn.execute("INSERT OR IGNORE INTO nav_dates (nav_date) VALUES (?)", (today_str,))
    if override_t1 and today_str in override_t1:
        conn.execute("UPDATE nav_dates SET t1_override=? WHERE nav_date=?",
                     (override_t1[today_str], today_str))
    conn.commit()

    cm = next((f for f in funds_data if f["fund_label"] == CM_LABEL), None)
    if cm:
        r7 = f"rank {cm.get('rank_7d','?')}/{n_funds}" if cm.get('rank_7d') else "unranked"
        print(f"  CM ranks → 7D:{r7}")

    print(f"\n{'='*50}")
    print(f"  Done. DB: {DB_PATH}")
    print(f"  NAV updated : {nav_count} funds")
    print(f"  AUM updated : {aum_count} funds (latest: {today_str})")
    print(f"{'='*50}")
    conn.close()


if __name__ == "__main__":
    full = "--full" in sys.argv
    ingest(full_history=full)
