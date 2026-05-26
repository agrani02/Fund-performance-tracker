#!/usr/bin/env python3
"""
Fund Performance Tracker — Flask API server
Endpoints: liquid fund NAV rankings, arbitrage fund rankings,
           liquid AUM tracker, ARB AUM tracker, T-1 overrides.
Runs on port 8081.
"""
import sqlite3, os, sys, datetime
from collections import defaultdict

try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
except ImportError:
    print("ERROR: pip install flask flask-cors"); sys.exit(1)

FRONTEND_DIR = os.path.dirname(os.path.abspath(__file__))
NAV_DB_PATH  = os.path.join(FRONTEND_DIR, "nav_rankings.db")
ARB_DB_PATH  = os.path.join(FRONTEND_DIR, "arb_rankings.db")
MULTIASSET_DB_PATH = os.path.join(FRONTEND_DIR, "multiasset_rankings.db")
FLEXI_DB_PATH      = os.path.join(FRONTEND_DIR, "flexi_rankings.db")

app = Flask(__name__, static_folder=None)

@app.after_request
def no_cache(response):
    """Prevent browser caching of all API responses."""
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response
CORS(app)


# ── DB Helpers ────────────────────────────────────────────────────────────────

def get_nav_conn():
    conn = sqlite3.connect(NAV_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA read_uncommitted=0")
    return conn

def get_arb_conn():
    conn = sqlite3.connect(ARB_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA read_uncommitted=0")
    return conn

def get_multiasset_conn():
    conn = sqlite3.connect(MULTIASSET_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def get_flexi_conn():
    conn = sqlite3.connect(FLEXI_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Shared AUM Tracker Logic ─────────────────────────────────────────────────

def _aum_tracker_data(conn):
    """Shared logic: build AUM tracker response from a DB connection."""
    import datetime as dt
    all_dates = [r["nav_date"] for r in conn.execute("""
        SELECT DISTINCT nav_date FROM nav_rankings
        WHERE aum_cr IS NOT NULL ORDER BY nav_date DESC
    """).fetchall()]
    if not all_dates:
        return None
    latest    = all_dates[0]
    latest_dt = dt.date.fromisoformat(latest)

    def find_on_or_before(target_dt):
        """Find the most recent available date that is <= target_dt.
        This ensures 7D always looks BACK, never forward."""
        candidates = [d for d in all_dates if dt.date.fromisoformat(d) <= target_dt]
        return candidates[0] if candidates else None  # all_dates is newest-first

    date_7d  = find_on_or_before(latest_dt - dt.timedelta(days=7))
    date_30d = find_on_or_before(latest_dt - dt.timedelta(days=30))

    rows = conn.execute("""
        SELECT fund_label, nav_date, aum_cr FROM nav_rankings
        WHERE aum_cr IS NOT NULL ORDER BY fund_label, nav_date DESC
    """).fetchall()

    fund_map = defaultdict(dict)
    for r in rows:
        fund_map[r["fund_label"]][r["nav_date"]] = r["aum_cr"]

    funds = []
    for label in sorted(fund_map.keys()):
        aum_by_date = fund_map[label]
        aum_now  = aum_by_date.get(latest)
        prev_7d  = aum_by_date.get(date_7d)
        prev_30d = aum_by_date.get(date_30d)
        chg_7d   = round((aum_now - prev_7d)  / prev_7d  * 100, 2) if aum_now and prev_7d  else None
        chg_30d  = round((aum_now - prev_30d) / prev_30d * 100, 2) if aum_now and prev_30d else None
        last_aum_date = max(aum_by_date.keys()) if aum_by_date else None
        funds.append({
            "fund_label":    label,
            "chg_7d":        chg_7d,
            "chg_30d":       chg_30d,
            "last_aum_date": last_aum_date,
            "aum_history":   {d: round(v, 2) for d, v in aum_by_date.items()},
        })
    return {"latest_date": latest, "date_7d": date_7d, "date_30d": date_30d,
            "all_dates": all_dates, "funds": funds}


# ── NAV (Liquid) Endpoints ────────────────────────────────────────────────────

@app.route("/api/nav/dates")
def nav_dates():
    try:
        if not os.path.exists(NAV_DB_PATH):
            return jsonify({"dates": []})
        conn = get_nav_conn()
        rows = conn.execute(
            "SELECT nav_date FROM nav_dates ORDER BY nav_date DESC LIMIT 120"
        ).fetchall()
        dates = [r["nav_date"] for r in rows]
        fcd = conn.execute("""
            SELECT nav_date FROM nav_rankings
            GROUP BY nav_date HAVING COUNT(*) >= 42 ORDER BY nav_date DESC LIMIT 1
        """).fetchone()
        full_count_date = fcd["nav_date"] if fcd else (dates[0] if dates else None)
        conn.close()
        return jsonify({"dates": dates, "full_count_date": full_count_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/nav/rankings")
def nav_rankings():
    try:
        date = request.args.get("date")
        if not date:
            return jsonify({"error": "Provide date"}), 400
        if not os.path.exists(NAV_DB_PATH):
            return jsonify({"error": "nav_rankings.db not found — run nav_ingest.py first"}), 404
        conn = get_nav_conn()
        meta = conn.execute(
            "SELECT t1_override FROM nav_dates WHERE nav_date=?", (date,)
        ).fetchone()
        t1_override = meta["t1_override"] if meta else None
        rows = conn.execute("""
            SELECT r.fund_label, r.nav_date AS fund_nav_date, r.nav, r.aum_cr, r.n_1d,
                   r.ret_1d, r.ret_7d, r.ret_30d, r.ret_91d, r.ret_184d, r.ret_365d, r.ret_si,
                   r.rank_1d, r.rank_7d, r.rank_30d, r.rank_91d, r.rank_184d, r.rank_365d, r.rank_si,
                   ln.last_nav_date, la.last_aum_date
            FROM nav_rankings r
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_nav_date
                FROM nav_rankings WHERE nav IS NOT NULL GROUP BY fund_label
            ) ln ON ln.fund_label = r.fund_label
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_aum_date
                FROM nav_rankings WHERE aum_cr IS NOT NULL GROUP BY fund_label
            ) la ON la.fund_label = r.fund_label
            WHERE r.nav_date = ?
            ORDER BY r.fund_label
        """, (date,)).fetchall()
        conn.close()
        return jsonify({
            "date":        date,
            "t1_override": t1_override,
            "funds":       [dict(r) for r in rows],
            "periods":     ["1d","7d","30d","91d","184d","365d","si"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/nav/aum-tracker")
def nav_aum_tracker():
    """Liquid fund AUM tracker — all history, 7D/30D % change."""
    try:
        if not os.path.exists(NAV_DB_PATH):
            return jsonify({"error": "nav_rankings.db not found — run nav_ingest.py first"}), 404
        conn = get_nav_conn()
        result = _aum_tracker_data(conn)
        conn.close()
        if not result:
            return jsonify({"error": "No AUM data found — run aum_backfill.py first"}), 404
        resp = jsonify(result)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/nav/override", methods=["POST"])
def nav_override():
    """Re-run ingest for a specific date with a custom T-1 date."""
    try:
        body      = request.get_json()
        nav_date  = body.get("nav_date")   # YYYY-MM-DD
        t1_date   = body.get("t1_date")    # YYYY-MM-DD
        if not nav_date or not t1_date:
            return jsonify({"error": "Provide nav_date and t1_date"}), 400
        sys.path.insert(0, FRONTEND_DIR)
        from nav_ingest import ingest
        ingest(override_t1={nav_date: t1_date})
        return jsonify({"success": True, "nav_date": nav_date, "t1_date": t1_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── ARB (Arbitrage) Endpoints ─────────────────────────────────────────────────

@app.route("/api/arb/dates")
def arb_dates():
    try:
        if not os.path.exists(ARB_DB_PATH):
            return jsonify({"dates": []})
        conn = get_arb_conn()
        rows = conn.execute(
            "SELECT nav_date FROM nav_dates ORDER BY nav_date DESC LIMIT 120"
        ).fetchall()
        dates = [r["nav_date"] for r in rows]
        fcd = conn.execute("""
            SELECT nav_date FROM nav_rankings
            GROUP BY nav_date HAVING COUNT(*) >= 37 ORDER BY nav_date DESC LIMIT 1
        """).fetchone()
        full_count_date = fcd["nav_date"] if fcd else (dates[0] if dates else None)
        conn.close()
        return jsonify({"dates": dates, "full_count_date": full_count_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/arb/rankings")
def arb_rankings():
    try:
        date = request.args.get("date")
        if not date:
            return jsonify({"error": "Provide date"}), 400
        if not os.path.exists(ARB_DB_PATH):
            return jsonify({"error": "arb_rankings.db not found — run arb_nav_ingest.py first"}), 404
        conn = get_arb_conn()
        meta = conn.execute(
            "SELECT t1_override FROM nav_dates WHERE nav_date=?", (date,)
        ).fetchone()
        t1_override = meta["t1_override"] if meta else None
        rows = conn.execute("""
            SELECT r.fund_label, r.nav_date AS fund_nav_date, r.nav, r.aum_cr, r.n_1d,
                   r.ret_1d, r.ret_7d, r.ret_30d, r.ret_91d, r.ret_184d, r.ret_365d, r.ret_si,
                   r.rank_1d, r.rank_7d, r.rank_30d, r.rank_91d, r.rank_184d, r.rank_365d, r.rank_si,
                   ln.last_nav_date, la.last_aum_date
            FROM nav_rankings r
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_nav_date
                FROM nav_rankings WHERE nav IS NOT NULL GROUP BY fund_label
            ) ln ON ln.fund_label = r.fund_label
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_aum_date
                FROM nav_rankings WHERE aum_cr IS NOT NULL GROUP BY fund_label
            ) la ON la.fund_label = r.fund_label
            WHERE r.nav_date = ?
            ORDER BY r.fund_label
        """, (date,)).fetchall()
        conn.close()
        return jsonify({
            "date":        date,
            "t1_override": t1_override,
            "funds":       [dict(r) for r in rows],
            "periods":     ["1d","7d","30d","91d","184d","365d","si"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/arb/aum-tracker")
def arb_aum_tracker():
    """ARB fund AUM tracker — all history, 7D/30D % change."""
    try:
        if not os.path.exists(ARB_DB_PATH):
            return jsonify({"error": "arb_rankings.db not found"}), 404
        conn = get_arb_conn()
        result = _aum_tracker_data(conn)
        conn.close()
        if not result:
            return jsonify({"error": "No ARB AUM data found — run aum_backfill.py first"}), 404
        resp = jsonify(result)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/arb/override", methods=["POST"])
def arb_override():
    """Re-run arbitrage ingest for a specific date with a custom T-1 date."""
    try:
        body      = request.get_json()
        nav_date  = body.get("nav_date")
        t1_date   = body.get("t1_date")
        if not nav_date or not t1_date:
            return jsonify({"error": "Provide nav_date and t1_date"}), 400
        sys.path.insert(0, FRONTEND_DIR)
        from arb_nav_ingest import ingest as arb_ingest
        arb_ingest(override_t1={nav_date: t1_date})
        return jsonify({"success": True, "nav_date": nav_date, "t1_date": t1_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── CM MULTI ASSET Endpoints ──────────────────────────────────────────────────

@app.route("/api/multiasset/dates")
def multiasset_dates():
    try:
        if not os.path.exists(MULTIASSET_DB_PATH):
            return jsonify({"dates": []})
        conn = get_multiasset_conn()
        rows = conn.execute("SELECT nav_date FROM nav_dates ORDER BY nav_date DESC LIMIT 120").fetchall()
        dates = [r["nav_date"] for r in rows]
        fcd = conn.execute("""
            SELECT nav_date FROM nav_rankings
            GROUP BY nav_date HAVING COUNT(*) >= 34 ORDER BY nav_date DESC LIMIT 1
        """).fetchone()
        full_count_date = fcd["nav_date"] if fcd else (dates[0] if dates else None)
        conn.close()
        return jsonify({"dates": dates, "full_count_date": full_count_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/multiasset/rankings")
def multiasset_rankings():
    try:
        date = request.args.get("date")
        if not date: return jsonify({"error": "Provide date"}), 400
        if not os.path.exists(MULTIASSET_DB_PATH):
            return jsonify({"error": "multiasset_rankings.db not found — run multiasset_daily.py first"}), 404
        conn = get_multiasset_conn()
        meta = conn.execute("SELECT t1_override FROM nav_dates WHERE nav_date=?", (date,)).fetchone()
        t1_override = meta["t1_override"] if meta else None
        rows = conn.execute("""
            SELECT r.fund_label, r.nav_date AS fund_nav_date, r.nav, r.aum_cr, r.n_1d,
                   r.ret_1d, r.ret_7d, r.ret_30d, r.ret_91d, r.ret_184d, r.ret_365d, r.ret_si,
                   r.rank_1d, r.rank_7d, r.rank_30d, r.rank_91d, r.rank_184d, r.rank_365d, r.rank_si,
                   ln.last_nav_date, la.last_aum_date
            FROM nav_rankings r
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_nav_date
                FROM nav_rankings WHERE nav IS NOT NULL GROUP BY fund_label
            ) ln ON ln.fund_label = r.fund_label
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_aum_date
                FROM nav_rankings WHERE aum_cr IS NOT NULL GROUP BY fund_label
            ) la ON la.fund_label = r.fund_label
            WHERE r.nav_date=? ORDER BY r.fund_label
        """, (date,)).fetchall()
        conn.close()
        return jsonify({"date": date, "t1_override": t1_override,
                        "funds": [dict(r) for r in rows],
                        "periods": ["1d","7d","30d","91d","184d","365d","si"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/multiasset/override", methods=["POST"])
def multiasset_override():
    try:
        body = request.get_json()
        nav_date = body.get("nav_date"); t1_date = body.get("t1_date")
        if not nav_date or not t1_date: return jsonify({"error": "Provide nav_date and t1_date"}), 400
        sys.path.insert(0, FRONTEND_DIR)
        from multiasset_daily import ingest
        ingest(override_t1={nav_date: t1_date})
        return jsonify({"success": True, "nav_date": nav_date, "t1_date": t1_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/multiasset/aum-tracker")
def multiasset_aum_tracker():
    try:
        if not os.path.exists(MULTIASSET_DB_PATH):
            return jsonify({"error": "multiasset_rankings.db not found"}), 404
        conn = get_multiasset_conn()
        result = _aum_tracker_data(conn)
        conn.close()
        if not result:
            return jsonify({"error": "No AUM data found — run multiasset_backfill.py first"}), 404
        resp = jsonify(result)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── CM FLEXI CAP Endpoints ───────────────────────────────────────────────────

@app.route("/api/flexi/dates")
def flexi_dates():
    try:
        if not os.path.exists(FLEXI_DB_PATH):
            return jsonify({"dates": []})
        conn = get_flexi_conn()
        rows = conn.execute("SELECT nav_date FROM nav_dates ORDER BY nav_date DESC LIMIT 120").fetchall()
        dates = [r["nav_date"] for r in rows]
        fcd = conn.execute("""
            SELECT nav_date FROM nav_rankings
            GROUP BY nav_date HAVING COUNT(*) >= 45 ORDER BY nav_date DESC LIMIT 1
        """).fetchone()
        full_count_date = fcd["nav_date"] if fcd else (dates[0] if dates else None)
        conn.close()
        return jsonify({"dates": dates, "full_count_date": full_count_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/flexi/rankings")
def flexi_rankings():
    try:
        date = request.args.get("date")
        if not date: return jsonify({"error": "Provide date"}), 400
        if not os.path.exists(FLEXI_DB_PATH):
            return jsonify({"error": "flexi_rankings.db not found — run flexi_daily.py first"}), 404
        conn = get_flexi_conn()
        meta = conn.execute("SELECT t1_override FROM nav_dates WHERE nav_date=?", (date,)).fetchone()
        t1_override = meta["t1_override"] if meta else None
        rows = conn.execute("""
            SELECT r.fund_label, r.nav_date AS fund_nav_date, r.nav, r.aum_cr, r.n_1d,
                   r.ret_1d, r.ret_7d, r.ret_30d, r.ret_91d, r.ret_184d, r.ret_365d, r.ret_si,
                   r.rank_1d, r.rank_7d, r.rank_30d, r.rank_91d, r.rank_184d, r.rank_365d, r.rank_si,
                   ln.last_nav_date, la.last_aum_date
            FROM nav_rankings r
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_nav_date
                FROM nav_rankings WHERE nav IS NOT NULL GROUP BY fund_label
            ) ln ON ln.fund_label = r.fund_label
            LEFT JOIN (
                SELECT fund_label, MAX(nav_date) AS last_aum_date
                FROM nav_rankings WHERE aum_cr IS NOT NULL GROUP BY fund_label
            ) la ON la.fund_label = r.fund_label
            WHERE r.nav_date=? ORDER BY r.fund_label
        """, (date,)).fetchall()
        conn.close()
        return jsonify({"date": date, "t1_override": t1_override,
                        "funds": [dict(r) for r in rows],
                        "periods": ["1d","7d","30d","91d","184d","365d","si"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/flexi/override", methods=["POST"])
def flexi_override():
    try:
        body = request.get_json()
        nav_date = body.get("nav_date"); t1_date = body.get("t1_date")
        if not nav_date or not t1_date: return jsonify({"error": "Provide nav_date and t1_date"}), 400
        sys.path.insert(0, FRONTEND_DIR)
        from flexi_daily import ingest
        ingest(override_t1={nav_date: t1_date})
        return jsonify({"success": True, "nav_date": nav_date, "t1_date": t1_date})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/flexi/aum-tracker")
def flexi_aum_tracker():
    try:
        if not os.path.exists(FLEXI_DB_PATH):
            return jsonify({"error": "flexi_rankings.db not found"}), 404
        conn = get_flexi_conn()
        result = _aum_tracker_data(conn)
        conn.close()
        if not result:
            return jsonify({"error": "No AUM data found — run flexi_backfill.py first"}), 404
        resp = jsonify(result)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def serve():
    for name in ["fpt_dashboard_full.html", "Fund Performance Tracker.html", "dashboard.html", "index.html"]:
        path = os.path.join(FRONTEND_DIR, name)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}
    return "No HTML found", 404


if __name__ == "__main__":
    print("="*50)
    print("  Fund Performance Tracker — http://localhost:8082")
    print("="*50)
    app.run(debug=True, host="0.0.0.0", port=8082)
