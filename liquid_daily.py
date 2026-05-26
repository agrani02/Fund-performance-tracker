#!/usr/bin/env python3
"""
NAV Rankings Ingest — replicates the Pivot sheet calculation exactly.

Formula (from Pivot):
    return = (NAV_today / NAV_prev - 1) * (365 / n_days)
    where n_days = actual calendar days between today and the lookback date
    Ranking: RANK() descending — higher annualised return = rank 1

Periods:
    1D   = previous trading day (auto n: 1 normal, 3 Monday, 4+ post-holiday)
    7D   = closest NAV to 7 calendar days ago,  n = actual gap
    30D  = closest NAV to 30 calendar days ago, n = actual gap
    91D  = closest NAV to 91 calendar days ago, n = actual gap
    184D = closest NAV to 184 calendar days ago, n = actual gap
    365D = closest NAV to 365 calendar days ago, n = actual gap
    SI   = NAV on 01-Dec-2025 (nearest trading day), n = today - 01-Dec-2025

Run daily after update_liquid_nav.py and AUM_timeseries.py:
    python nav_ingest.py
"""
import sqlite3, os, datetime
import pandas as pd

BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
AMFI_NAV_URL   = "https://portal.amfiindia.com/spages/NAVOpen.txt"
AMFI_HIST_URL  = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
AMFI_AUM_URL   = "https://www.amfiindia.com/gateway/pollingsebi/api/amfi/fundperformance"
DB_PATH        = os.path.join(BASE_DIR, "nav_rankings.db")
INCEPTION_DATE = datetime.date(2025, 12, 1)   # hardcoded per Pivot

# Target periods: label → nominal calendar days for lookback search
PERIODS = {
    "1d":   1,
    "7d":   7,
    "30d":  30,
    "91d":  91,
    "184d": 184,
    "365d": 365,
    "si":   None,   # since inception — special handling
}

# NAV file schemeName → short display label (uppercase substring match)
FUND_MAP = {
    "360 ONE":              "360 ONE",
    "ABAKKUS":              "Abakkus",
    "ADITYA BIRLA":         "Aditya Birla Sun Life",
    "AXIS LIQUID":          "Axis",
    "BAJAJ FINSERV":        "Bajaj Finserv",
    "BANDHAN":              "Bandhan",
    "BANK OF INDIA":        "Bank of India",
    "BARODA BNP":           "Baroda BNP Paribas",
    "CANARA ROBECO":        "Canara Robeco",
    "CAPITALMIND":          "Capitalmind",
    "DSP LIQUID":           "DSP",
    "EDELWEISS":            "Edelweiss",
    "FRANKLIN":             "Franklin Templeton",
    "GROWW":                "Groww",
    "HDFC LIQUID":          "HDFC",
    "HSBC LIQUID":          "HSBC",
    "ICICI PRUDENTIAL":     "ICICI P",
    "INVESCO":              "Invesco",
    "ITI LIQUID":           "ITI",
    "JIOBLACKROCK":         "Jio B",
    "JIO BLACKROCK":        "Jio B",
    "JM LIQUID":            "JM Financial",
    "KOTAK LIQUID":         "Kotak Mahindra",
    "LIC MF":               "LIC",
    "MAHINDRA MANULIFE":    "Mahindra Manulife",
    "MIRAE ASSET":          "Mirae Asset",
    "MOTILAL OSWAL":        "Motilal Oswal",
    "NAVI LIQUID":          "Navi",
    "NIPPON INDIA LIQUID":  "Nippon India",
    "PGIM INDIA":           "PGIM India",
    "PARAG PARIKH":         "PPFAS",
    "QUANTUM LIQUID":       "Quantum",
    "QUANT LIQUID":         "quant",
    "SBI LIQUID":           "SBI",
    "SHRIRAM LIQUID":       "Shriram",
    "SUNDARAM LIQUID":      "Sundaram",
    "TATA LIQUID":          "Tata",
    "THE WEALTH COMPANY":   "The Wealth Company",
    "TRUSTMF":              "Trust",
    "UNIFI LIQUID":         "Unifi",
    "UNION LIQUID":         "Union",
    "UTI":                  "UTI",
    "WHITEOAK CAPITAL":     "WhiteOak Capital",
}

AUM_MAP = {**FUND_MAP,
    "INVESCO INDIA": "Invesco",
    "DSP LIQUIDITY": "DSP",
}


# ── Helpers ────────────────────────────────────────────────────────────────

def map_name(raw, mapping):
    u = str(raw).upper()
    for key, label in mapping.items():
        if key in u:
            return label
    return None


def parse_date(d):
    if isinstance(d, (datetime.date, datetime.datetime)):
        return pd.Timestamp(d)
    for fmt in ("%d-%m-%Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return pd.Timestamp(datetime.datetime.strptime(str(d).strip(), fmt))
        except ValueError:
            pass
    return pd.NaT


def get_nav_for_target(fund_ts, target_date, search_window=5):
    """
    Find the closest available NAV to target_date within ±search_window days.
    Skips Saturday only (Saturday has no NAV but Sunday does).
    Returns (nav_value, actual_date) or (None, None).
    """
    lo = target_date - pd.Timedelta(days=search_window)
    hi = target_date + pd.Timedelta(days=search_window)
    window = fund_ts[
        (fund_ts.index >= lo) &
        (fund_ts.index <= hi) &
        (fund_ts.index.dayofweek != 5)    # skip Saturday
    ].dropna()
    if window.empty:
        return None, None
    diffs = [(abs((idx - target_date).days), idx) for idx in window.index]
    _, closest_date = min(diffs, key=lambda x: x[0])
    return window[closest_date], closest_date


def get_prev_trading_nav(fund_ts, today, override_date=None):
    """
    Get T-1 NAV and actual calendar gap (n).
    Rules:
      - If override_date is set, use that date directly (holiday override)
      - Sunday in file → skip Saturday, use Thursday (n=3)
      - All other days → previous available date in file
    """
    def _get_scalar(ts, idx):
        """Safely get scalar value from series at index — avoids Series truth value error."""
        val = ts[idx]
        if isinstance(val, pd.Series):
            val = val.iloc[-1]
        return val

    if override_date is not None:
        target = pd.Timestamp(override_date)
        if target in fund_ts.index:
            val = _get_scalar(fund_ts, target)
            if pd.notna(val):
                return float(val), int((today - target).days)
        for d in range(1, 3):
            for delta in [pd.Timedelta(days=d), pd.Timedelta(days=-d)]:
                check = target + delta
                if check in fund_ts.index:
                    val = _get_scalar(fund_ts, check)
                    if pd.notna(val):
                        return float(val), int((today - check).days)
        return None, None

    if today.dayofweek == 6:
        # Sunday → use Thursday
        target = today - pd.Timedelta(days=3)
        if target in fund_ts.index:
            val = _get_scalar(fund_ts, target)
            if pd.notna(val):
                return float(val), 3
        prior = fund_ts[(fund_ts.index < today) &
                        (fund_ts.index.dayofweek != 5)].dropna()
    else:
        prior = fund_ts[fund_ts.index < today].dropna()

    if prior.empty:
        return None, None
    prev_date = prior.index[-1]
    n = int((today - prev_date).days)
    return float(prior.iloc[-1]), n


def annualised_return(nav_today, nav_prev, n_days):
    """Exact Pivot formula: (NAV_t / NAV_t-n - 1) * (365 / n)"""
    if nav_today is None or nav_prev is None or nav_prev == 0 or n_days is None or n_days == 0:
        return None
    return (nav_today / nav_prev - 1) * (365 / n_days) * 100


# ── DB ─────────────────────────────────────────────────────────────────────

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nav_rankings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nav_date    TEXT NOT NULL,
            fund_label  TEXT NOT NULL,
            nav         REAL,
            aum_cr      REAL,
            ret_1d      REAL,  ret_7d   REAL,  ret_30d  REAL,
            ret_91d     REAL,  ret_184d REAL,  ret_365d REAL,  ret_si REAL,
            rank_1d     INTEGER, rank_7d   INTEGER, rank_30d  INTEGER,
            rank_91d    INTEGER, rank_184d INTEGER, rank_365d INTEGER, rank_si INTEGER,
            n_1d        INTEGER,
            UNIQUE(nav_date, fund_label)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nav_dates (
            nav_date    TEXT PRIMARY KEY,
            t1_override TEXT,
            ingested_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # ── Migrations: add columns to existing DBs ──
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(nav_dates)")}
    if "t1_override" not in existing_cols:
        conn.execute("ALTER TABLE nav_dates ADD COLUMN t1_override TEXT")
    conn.commit()


# ── Main ───────────────────────────────────────────────────────────────────

# Hardcoded scheme codes for all liquid direct funds
# (from liquid_direct_nav.xlsx — update if new funds are added)
SCHEME_CODES = {
    125345: "360 ONE",
    154051: "Abakkus",
    119568: "Aditya Birla Sun Life",
    120389: "Axis",
    151833: "Bajaj Finserv",
    118364: "Bandhan",
    119369: "Bank of India",
    119415: "Baroda BNP Paribas",
    118305: "Canara Robeco",
    154011: "Capitalmind",
    119125: "DSP",
    140196: "Edelweiss",
    118577: "Franklin Templeton",
    119135: "Groww",
    119091: "HDFC",
    120038: "HSBC",
    120197: "ICICI P",
    120537: "Invesco",
    147157: "ITI",
    153651: "Jio B",
    120406: "JM Financial",
    119766: "Kotak Mahindra",
    120249: "LIC",
    139538: "Mahindra Manulife",
    118859: "Mirae Asset",
    145834: "Motilal Oswal",
    119164: "Navi",
    118701: "Nippon India",
    138299: "PGIM India",
    143269: "PPFAS",
    103734: "Quantum",
    120837: "quant",
    119800: "SBI",
    153035: "Shriram",
    149664: "Sundaram",
    119861: "Tata",
    153883: "The Wealth Company",
    148841: "Trust",
    153570: "Unifi",
    119303: "Union",
    120304: "UTI",
    145971: "WhiteOak Capital",
}


def _parse_amfi_nav_text(text, is_history=False):
    """Parse AMFI NAV text file (semicolon-delimited).

    NAVOpen.txt format (daily):
        Scheme Code;ISIN Growth;ISIN Reinvest;Scheme Name;Net Asset Value;Date
        → code=0, name=3, nav=4, date=5

    History format (DownloadNAVHistoryReport):
        Scheme Code;Scheme Name;ISIN Growth;ISIN Reinvest;Net Asset Value;Repurchase;Sale;Date
        → code=0, name=1, nav=4, date=7

    Returns list of dicts with scheme_code, nav, date_parsed, fund_label."""
    rows = []
    scheme_codes_str = {str(k): v for k, v in SCHEME_CODES.items()}

    if is_history:
        name_idx, nav_idx, date_idx, min_parts = 1, 4, 7, 8
    else:
        name_idx, nav_idx, date_idx, min_parts = 3, 4, 5, 6

    for line in text.strip().split("\n"):
        parts = line.strip().split(";")
        if len(parts) < min_parts:
            continue
        code_str = parts[0].strip()
        if code_str not in scheme_codes_str:
            continue
        try:
            nav_val = float(parts[nav_idx].strip())
        except (ValueError, TypeError):
            continue
        date_str = parts[date_idx].strip()
        dt_parsed = parse_date(date_str)
        if pd.isna(dt_parsed):
            continue
        rows.append({
            "schemeCode": int(code_str),
            "schemeName": parts[name_idx].strip(),
            "date": date_str,
            "nav": nav_val,
            "fund_label": scheme_codes_str[code_str],
            "date_parsed": dt_parsed,
        })
    return rows


def fetch_nav_from_api(full_history=False):
    """Fetch NAV for all liquid direct funds from AMFI portal.

    full_history=False (daily): fetches NAVOpen.txt — single file, fast.
    full_history=True  (full rebuild): fetches history in 90-day chunks
        from AMFI DownloadNAVHistoryReport endpoint.
    """
    import requests, time as _time
    LOOKBACK_DAYS = 370   # enough for 365D return calculation

    if not full_history:
        # ── DAILY: NAVOpen.txt — single file download ──
        print("Fetching NAV from AMFI NAVOpen.txt ...")
        for attempt in range(1, 4):
            try:
                resp = requests.get(AMFI_NAV_URL, timeout=60)
                resp.raise_for_status()
                rows = _parse_amfi_nav_text(resp.text)
                if rows:
                    break
                print(f"  Attempt {attempt}/3: no matching schemes found")
            except Exception as e:
                print(f"  Attempt {attempt}/3 failed: {e}")
                if attempt == 3:
                    print("  ERROR: All retries failed"); return pd.DataFrame()
                _time.sleep(5 * attempt)

        if not rows:
            print("  ERROR: No scheme codes matched"); return pd.DataFrame()

        matched = {r["fund_label"] for r in rows}
        missing = set(SCHEME_CODES.values()) - matched
        if missing:
            print(f"  WARN: {len(missing)} schemes not found: {sorted(missing)}")

    else:
        # ── FULL HISTORY: fetch in 90-day chunks from AMFI ──
        lookback_start = (pd.Timestamp.today().normalize() - pd.Timedelta(days=LOOKBACK_DAYS)).date()
        today = datetime.date.today()
        print(f"Fetching full NAV history from AMFI (back to {lookback_start})...")
        print(f"Using 90-day chunks from portal.amfiindia.com...")
        rows = []

        chunk_start = lookback_start
        chunk_num = 0
        while chunk_start <= today:
            chunk_end = min(chunk_start + datetime.timedelta(days=89), today)
            chunk_num += 1
            frmdt = chunk_start.strftime("%d-%b-%Y")
            todt = chunk_end.strftime("%d-%b-%Y")

            for attempt in range(1, 4):
                try:
                    resp = requests.get(
                        AMFI_HIST_URL,
                        params={"tp": 1, "frmdt": frmdt, "todt": todt},
                        timeout=60
                    )
                    resp.raise_for_status()
                    chunk_rows = _parse_amfi_nav_text(resp.text, is_history=True)
                    rows.extend(chunk_rows)
                    n_dates = len({r["date"] for r in chunk_rows})
                    print(f"  Chunk {chunk_num}: {frmdt} → {todt} — {len(chunk_rows)} rows, {n_dates} dates")
                    break
                except Exception as e:
                    if attempt == 3:
                        print(f"  Chunk {chunk_num}: {frmdt} → {todt} — FAILED: {e}")
                    else:
                        _time.sleep(3 * attempt)
            _time.sleep(0.5)  # polite delay between chunks
            chunk_start = chunk_end + datetime.timedelta(days=1)

    if not rows:
        print("  ERROR: No NAV data fetched"); return pd.DataFrame()

    df = pd.DataFrame(rows)
    if "date_parsed" not in df.columns:
        df["date_parsed"] = df["date"].apply(parse_date)
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna(subset=["date_parsed", "nav"])

    n_funds = df["fund_label"].nunique()
    d_min   = df["date_parsed"].min().date()
    d_max   = df["date_parsed"].max().date()
    print(f"  {n_funds}/{len(SCHEME_CODES)} funds fetched  |  {d_min} → {d_max}")
    return df


def fetch_aum_from_api(category=2, subcategory=20, aum_col="dailyAUM"):
    """Fetch latest AUM from AMFI.
    Tries today and each prior day (up to 10 days back) until data found.
    AUM: dailyAUM column (Daily AUM).
    """
    import requests, datetime as dt
    print("Fetching AUM from AMFI...")

    cookies = {
        "__gsas": "ID=f8678d6ec5abaf6c:T=1764739479:RT=1764739479:S=ALNI_MZB7yKBaAgnLq05icnN8Pcx2bKEHA",
    }
    headers = {
        "Accept":       "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin":       "https://www.amfiindia.com",
        "Referer":      "https://www.amfiindia.com/polling/amfi/fund-performance",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    }

    today = dt.date.today()
    # Try today and each day back up to 10 days — AUM is daily, 2-day lag typical
    dates_to_try = [
        (today - dt.timedelta(days=i)).strftime("%d-%b-%Y")
        for i in range(0, 10)
    ]

    for report_date in dates_to_try:
        try:
            resp = requests.post(
                AMFI_AUM_URL,
                cookies=cookies,
                headers=headers,
                json={"maturityType": 1, "category": category,
                      "subCategory": subcategory, "mfid": 0,
                      "reportDate": report_date},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                print(f"  {report_date}: no data, trying previous day...")
                continue

            df_raw = pd.DataFrame(items)
            # Use dailyAUM (Daily AUM)
            col = "dailyAUM" if "dailyAUM" in df_raw.columns else None
            if "schemeName" not in df_raw.columns or col is None:
                print(f"  WARN: unexpected columns: {df_raw.columns.tolist()}")
                continue

            rows = []
            for _, row in df_raw.iterrows():
                name  = str(row.get("schemeName", ""))
                aum   = row.get(col)
                label = map_name(name, AUM_MAP)
                if label and aum:
                    try:
                        rows.append({"fund_label": label, "aum_cr": float(aum)})
                    except (ValueError, TypeError):
                        pass

            if rows:
                df = pd.DataFrame(rows).drop_duplicates(subset=["fund_label"], keep="last")
                print(f"  AUM fetched for {report_date}: {len(df)} funds")
                return df, report_date

        except Exception as e:
            print(f"  WARN: AUM fetch for {report_date} failed: {e}")

    print("  WARNING: Could not fetch AUM — rankings will show without AUM")
    return pd.DataFrame(columns=["fund_label", "aum_cr"]), None


def ingest(override_t1=None, full_history=False):
    """
    override_t1:   dict of {nav_date_str: t1_date_str} for holiday overrides
    full_history:  True = fetch full per-scheme history (for --full rebuild)
                   False = fetch only latest NAV via /mf/latest (daily update)
    """
    if override_t1 is None:
        override_t1 = {}

    nav_raw = fetch_nav_from_api(full_history=full_history)
    if nav_raw.empty:
        print("ERROR: No NAV data available"); return

    aum_raw, aum_date = fetch_aum_from_api()
    if not aum_raw.empty:
        print(f"  AUM date: {aum_date}  |  {len(aum_raw)} funds loaded")
    else:
        print("  WARNING: No AUM data — rankings will have no AUM")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    conn.row_factory = sqlite3.Row
    done = {r["nav_date"] for r in conn.execute("SELECT nav_date FROM nav_dates")}
    # Dates with overrides should always be reprocessed
    done -= set(override_t1.keys())
    # Always reprocess dates returned by today's API fetch (fresh data)
    api_dates = {d.strftime("%Y-%m-%d") for d in nav_raw["date_parsed"].unique()}
    done -= api_dates

    all_dates = sorted(nav_raw["date_parsed"].unique())
    new_dates  = [d for d in all_dates if d.strftime("%Y-%m-%d") not in done]
    print(f"  {len(new_dates)} new dates to process (of {len(all_dates)} total)\n")

    inception_ts = pd.Timestamp(INCEPTION_DATE)

    # ── Load historical NAVs for multi-period return calculation ──
    # full_history=True: nav_raw has all history → build from nav_raw
    # full_history=False: nav_raw only has today → load from DB
    import collections as _col
    hist_nav = {}   # {fund_label: pd.Series(nav, index=date)}

    if full_history:
        # Build from nav_raw — has full history already
        raw_hist = _col.defaultdict(dict)
        for _, row in nav_raw.iterrows():
            raw_hist[row["fund_label"]][row["date_parsed"]] = row["nav"]
        for label, date_nav in raw_hist.items():
            s = pd.Series(date_nav)
            s.index = pd.DatetimeIndex(s.index)
            hist_nav[label] = s.sort_index()
        print(f"  Built history from fetched NAV data: {len(hist_nav)} funds")
    else:
        # Daily mode — load from DB (has previous days)
        try:
            conn.row_factory = sqlite3.Row
            rows_hist = conn.execute("""
                SELECT nav_date, fund_label, nav FROM nav_rankings
                ORDER BY nav_date
            """).fetchall()
            raw_hist = _col.defaultdict(dict)
            for r in rows_hist:
                raw_hist[r["fund_label"]][pd.Timestamp(r["nav_date"])] = r["nav"]
            for label, date_nav in raw_hist.items():
                s = pd.Series(date_nav)
                s.index = pd.DatetimeIndex(s.index)
                hist_nav[label] = s.sort_index()
            print(f"  Loaded historical NAVs from DB: {len(hist_nav)} funds")
        except Exception as e:
            print(f"  WARN: Could not load historical NAVs from DB: {e}")

    for nav_date in new_dates:
        date_str = nav_date.strftime("%Y-%m-%d")
        day_nav  = nav_raw[nav_raw["date_parsed"] == nav_date]
        rows = []

        for _, row in day_nav.iterrows():
            label   = row["fund_label"]
            nav_val = row["nav"]

            # Build time series: merge DB history + today's API fetch
            if label in hist_nav:
                fund_ts = hist_nav[label].copy()
                fund_ts[nav_date] = nav_val
                fund_ts = fund_ts.sort_index()
            else:
                fund_ts = pd.Series({nav_date: nav_val})
                fund_ts.index = pd.DatetimeIndex(fund_ts.index)

            # Drop Saturday
            fund_ts = fund_ts[fund_ts.index.dayofweek != 5]

            # ── 1D ──
            override = override_t1.get(date_str)
            nav_1d, n_1d = get_prev_trading_nav(fund_ts, nav_date, override_date=override)
            ret_1d = annualised_return(nav_val, nav_1d, n_1d)

            # ── 7D, 30D, 91D, 184D, 365D ──
            rets = {"1d": ret_1d}
            for label_p, nominal in [("7d",7),("30d",30),("91d",91),("184d",184),("365d",365)]:
                target     = nav_date - pd.Timedelta(days=nominal)
                nav_p, actual_date = get_nav_for_target(fund_ts, target)
                if actual_date is not None:
                    n_actual = (nav_date - actual_date).days
                else:
                    n_actual = nominal
                rets[label_p] = annualised_return(nav_val, nav_p, n_actual)

            # ── Since Inception ──
            nav_si, si_date = get_nav_for_target(fund_ts, inception_ts, search_window=5)
            n_si = (nav_date - inception_ts).days if nav_si is not None else None
            rets["si"] = annualised_return(nav_val, nav_si, n_si)

            # ── AUM: from API fetch (already latest available) ──
            aum = None
            if not aum_raw.empty:
                sub = aum_raw[aum_raw["fund_label"] == label]
                if not sub.empty:
                    aum = sub.iloc[0]["aum_cr"]

            rows.append({
                "fund_label": label, "nav": nav_val, "aum_cr": aum,
                "n_1d": n_1d,
                **{f"ret_{p}": rets[p] for p in ["1d","7d","30d","91d","184d","365d","si"]}
            })

        if not rows:
            continue

        df_day = pd.DataFrame(rows)

        # Rank per period — higher return = rank 1 (matches Excel RANK descending)
        for p in ["1d","7d","30d","91d","184d","365d","si"]:
            col_r = f"ret_{p}"
            col_k = f"rank_{p}"
            valid = df_day[col_r].notna()
            if valid.any():
                df_day.loc[valid, col_k] = (
                    df_day.loc[valid, col_r]
                    .rank(ascending=False, method="min")
                    .astype(int)
                )

        # Write to DB
        for _, r in df_day.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO nav_rankings
                (nav_date, fund_label, nav, aum_cr,
                 ret_1d, ret_7d, ret_30d, ret_91d, ret_184d, ret_365d, ret_si,
                 rank_1d, rank_7d, rank_30d, rank_91d, rank_184d, rank_365d, rank_si,
                 n_1d)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                date_str, r["fund_label"], r["nav"], r.get("aum_cr"),
                r.get("ret_1d"), r.get("ret_7d"), r.get("ret_30d"),
                r.get("ret_91d"), r.get("ret_184d"), r.get("ret_365d"), r.get("ret_si"),
                r.get("rank_1d"), r.get("rank_7d"), r.get("rank_30d"),
                r.get("rank_91d"), r.get("rank_184d"), r.get("rank_365d"), r.get("rank_si"),
                r.get("n_1d"),
            ))

        conn.execute(
            "INSERT OR REPLACE INTO nav_dates (nav_date, t1_override) VALUES (?,?)",
            (date_str, override_t1.get(date_str))
        )
        conn.commit()

        # Show Capitalmind ranks on this date
        cm = df_day[df_day["fund_label"] == "Capitalmind"]
        if not cm.empty:
            c = cm.iloc[0]
            print(f"  OK {date_str}  {len(rows)} funds  "
                  f"| CM ranks -> 1D:{int(c['rank_1d']) if c.get('rank_1d') is not None and str(c.get('rank_1d')) != 'nan' else '-'} "
                  f"7D:{int(c['rank_7d']) if c.get('rank_7d') is not None and str(c.get('rank_7d')) != 'nan' else '-'} "
                  f"30D:{int(c['rank_30d']) if c.get('rank_30d') is not None and str(c.get('rank_30d')) != 'nan' else '-'} "
                  f"91D:{int(c['rank_91d']) if c.get('rank_91d') is not None and str(c.get('rank_91d')) != 'nan' else '-'} "
                  f"SI:{int(c['rank_si']) if c.get('rank_si') is not None and str(c.get('rank_si')) != 'nan' else '-'})")
        else:
            print(f"  OK {date_str}  {len(rows)} funds")

    conn.close()
    # Print clear summary
    import sqlite3 as _sq2
    _c2 = _sq2.connect(DB_PATH)
    _latest = _c2.execute("SELECT MAX(nav_date) FROM nav_rankings WHERE aum_cr IS NOT NULL").fetchone()[0]
    _aum_count = _c2.execute("SELECT COUNT(*) FROM nav_rankings WHERE nav_date=? AND aum_cr IS NOT NULL", (_latest,)).fetchone()[0]
    _nav_count = _c2.execute("SELECT COUNT(*) FROM nav_rankings WHERE nav_date=? AND nav IS NOT NULL", (_latest,)).fetchone()[0]
    _c2.close()
    print(f"\n{'='*50}")
    print(f"  Done. DB: {DB_PATH}")
    print(f"  NAV updated : {_nav_count} funds")
    print(f"  AUM updated : {_aum_count} funds (latest: {_latest})")
    print(f"{'='*50}")


if __name__ == "__main__":
    print("=" * 50)
    print("  LIQUID FUND — Daily Update")
    print("=" * 50)
    ingest(full_history=False)
