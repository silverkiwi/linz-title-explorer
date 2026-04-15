import base64
import csv
import io
import os
import re
from contextlib import contextmanager
from urllib.parse import unquote

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request

load_dotenv()
app = Flask(__name__)

SF_ACCOUNT   = os.environ.get("SF_ACCOUNT", "")
SF_USER      = os.environ.get("SF_USER", "")
SF_DATABASE  = os.environ.get("SF_DATABASE", "L")
SF_WAREHOUSE = os.environ.get("SF_WAREHOUSE", "COMPUTE_WH")
# SF_PRIVATE_KEY: base64-encoded PEM (used in production / Vercel)
# SF_KEY_PATH: path to .p8 file (used locally)
SF_PRIVATE_KEY = os.environ.get("SF_PRIVATE_KEY", "")
SF_KEY_PATH    = os.environ.get("SF_KEY_PATH", os.path.join(os.path.dirname(__file__), "keys/snowflake_rsa.p8"))

def _load_private_key():
    if SF_PRIVATE_KEY:
        pem = base64.b64decode(SF_PRIVATE_KEY)
    else:
        with open(SF_KEY_PATH, "rb") as f:
            pem = f.read()
    return serialization.load_pem_private_key(pem, password=None)

# Title numbers can contain spaces, commas, parens, slashes, etc (old paper title formats).
# Security is handled by _sql_literal(); we just block null bytes and raw quotes.
TITLE_RE = re.compile(r"^[^\x00\n\r'\"\\]{1,100}$")


def _parse_title_no(raw: str) -> str | None:
    """URL-decode (handles %2F from browsers) and validate title_no.
    Returns the decoded title string, or None if it fails validation."""
    t = unquote(raw)
    return t if TITLE_RE.match(t) else None

SEARCH_SORT_COLUMNS = {
    "title_no":          "TITLE_NO",
    "issue_date":        "ISSUE_DATE",
    "latest_lodged_date": "LATEST_INSTRUMENT_DATE",
    "instrument_count":  "INSTRUMENT_COUNT",
    "encumbrance_count": "ENCUMBRANCE_COUNT",
    "updated_at":        "REFRESHED_AT",
}


@contextmanager
def get_conn():
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=_load_private_key(),
        database=SF_DATABASE,
        warehouse=SF_WAREHOUSE,
    )
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_dicts(cursor):
    cols = [c[0].lower() for c in cursor.description]
    out = []
    for row in cursor.fetchall():
        item = {}
        for i, col in enumerate(cols):
            val = row[i]
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            item[col] = val
        out.append(item)
    return out


def _query(sql: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        data = _rows_to_dicts(cur)
        cur.close()
    return data


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _to_int(value, default: int, min_v: int, max_v: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except Exception:
        parsed = default
    return min(max(parsed, min_v), max_v)


def _to_csv_response(rows: list, filename: str) -> Response:
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    else:
        output.write("\n")
    csv_data = output.getvalue()
    output.close()
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ---------------------------------------------------------------------------
# Title search against GOLD.V_TITLE_360
# ---------------------------------------------------------------------------

def _build_search_query(q: str, limit: int, offset: int, sort_by: str, sort_dir: str) -> str:
    where = ""
    if q:
        qlit = _sql_literal(f"%{q.lower()}%")
        where = f"""WHERE LOWER(TITLE_NO) LIKE {qlit}
                  OR LOWER(COALESCE(PRIMARY_ADDRESS, ''))  LIKE {qlit}
                  OR LOWER(COALESCE(APPELLATIONS, ''))     LIKE {qlit}
                  OR LOWER(COALESCE(PROPRIETORS, ''))      LIKE {qlit}
                  OR LOWER(COALESCE(ESTATE_TYPES, ''))     LIKE {qlit}"""

    safe_col = SEARCH_SORT_COLUMNS.get(sort_by, "ISSUE_DATE")
    safe_dir = "ASC" if sort_dir.upper() == "ASC" else "DESC"

    return f"""
    SELECT
        TITLE_NO,
        TITLE_STATUS,
        TITLE_STATUS_DESC,
        TITLE_TYPE,
        TITLE_TYPE_DESC,
        REGISTER_TYPE,
        REGISTER_TYPE_DESC,
        ISSUE_DATE,
        IS_CURRENT,
        IS_ACTIVE,
        LATEST_INST_NO          AS latest_instrument_no,
        LATEST_INST_TYPE        AS latest_instrument_type,
        LATEST_INST_TYPE_DESC   AS latest_instrument_type_desc,
        LATEST_INSTRUMENT_DATE  AS latest_lodged_date,
        INSTRUMENT_COUNT,
        ENCUMBRANCE_COUNT,
        ESTATE_COUNT,
        ESTATE_TYPES,
        PARCEL_COUNT,
        PRIMARY_ADDRESS,
        APPELLATIONS,
        PRIOR_TITLE_NO,
        REFRESHED_AT            AS updated_at
    FROM L.GOLD.V_TITLE_360
    {where}
    ORDER BY IS_CURRENT DESC NULLS LAST, {safe_col} {safe_dir} NULLS LAST, TITLE_NO ASC
    LIMIT {limit}
    OFFSET {offset}
    """


# ---------------------------------------------------------------------------
# Address search against GOLD.DIM_ADDRESS
# ---------------------------------------------------------------------------

def _build_address_search_query(q: str, limit: int) -> str:
    where = ""
    if q:
        qlit = _sql_literal(f"%{q.lower()}%")
        where = f"""WHERE LOWER(FULL_ADDRESS) LIKE {qlit}
                   AND TITLE_NO IS NOT NULL"""
    else:
        where = "WHERE TITLE_NO IS NOT NULL"

    return f"""
    SELECT
        TITLE_NO,
        ADDRESS_ID,
        PARCEL_ID,
        FULL_ADDRESS,
        STREET_ADDRESS,
        UNIT_TYPE,
        UNIT_VALUE,
        STREET_NUMBER,
        STREET_NUMBER_HIGH,
        ROAD_NAME,
        ROAD_TYPE,
        SUBURB,
        CITY,
        POSTCODE
    FROM L.GOLD.DIM_ADDRESS
    {where}
    ORDER BY FULL_ADDRESS
    LIMIT {limit}
    """


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/health")
def health():
    if not (SF_ACCOUNT and SF_USER):
        return jsonify({"ok": False, "error": "Missing Snowflake env vars"}), 500
    data = _query("SELECT 1 AS ok")
    return jsonify({"ok": True, "snowflake": bool(data and data[0].get("ok") == 1)})


@app.get("/api/search")
def search():
    q        = (request.args.get("q") or "").strip()
    limit    = _to_int(request.args.get("limit"),    default=25,  min_v=1, max_v=200)
    offset   = _to_int(request.args.get("offset"),   default=0,   min_v=0, max_v=100_000)
    sort_by  = (request.args.get("sort_by")  or "issue_date").strip().lower()
    sort_dir = (request.args.get("sort_dir") or "desc").strip().upper()
    out = _query(_build_search_query(q, limit, offset, sort_by, sort_dir))
    return jsonify({"count": len(out), "items": out, "limit": limit, "offset": offset})


@app.get("/api/search.csv")
def search_csv():
    q        = (request.args.get("q") or "").strip()
    limit    = _to_int(request.args.get("limit"),    default=5000, min_v=1, max_v=10_000)
    offset   = _to_int(request.args.get("offset"),   default=0,    min_v=0, max_v=100_000)
    sort_by  = (request.args.get("sort_by")  or "issue_date").strip().lower()
    sort_dir = (request.args.get("sort_dir") or "desc").strip().upper()
    out = _query(_build_search_query(q, limit, offset, sort_by, sort_dir))
    return _to_csv_response(out, "linz_title_search.csv")


@app.get("/api/address/search")
def address_search():
    q     = (request.args.get("q") or "").strip()
    limit = _to_int(request.args.get("limit"), default=50, min_v=1, max_v=500)
    out   = _query(_build_address_search_query(q, limit))
    return jsonify({"count": len(out), "items": out})


def _require_title_no():
    """Read ?no= query param, validate, return (title_no, None) or (None, error_response)."""
    raw = (request.args.get("no") or "").strip()
    title_no = _parse_title_no(raw)
    if not title_no:
        return None, (jsonify({"error": "Invalid title_no format"}), 400)
    return title_no, None


@app.get("/api/title")
def title_detail():
    title_no, err = _require_title_no()
    if err:
        return err
    out = _query(f"""
        SELECT
            TITLE_NO,
            TITLE_STATUS, TITLE_STATUS_DESC,
            TITLE_TYPE, TITLE_TYPE_DESC,
            REGISTER_TYPE, REGISTER_TYPE_DESC,
            ISSUE_DATE, IS_CURRENT, IS_ACTIVE,
            GUARANTEE_STATUS, MAORI_LAND, LDT_LOC_ID, LAND_DISTRICT_NAME,
            ESTATE_COUNT, CURRENT_ESTATE_COUNT, ESTATE_TYPES,
            PROPRIETOR_COUNT, PROPRIETORS,
            INSTRUMENT_COUNT, LATEST_INSTRUMENT_DATE, LATEST_INST_NO,
            LATEST_INST_TYPE, LATEST_INST_TYPE_DESC,
            ENCUMBRANCE_COUNT, CURRENT_ENCUMBRANCE_COUNT,
            PARCEL_COUNT, PRIMARY_ADDRESS, APPELLATIONS,
            PRIOR_TITLE_NO, REFRESHED_AT
        FROM L.GOLD.V_TITLE_360
        WHERE TITLE_NO = {_sql_literal(title_no)}
        LIMIT 1
    """)
    if not out:
        return jsonify({"error": "Not found"}), 404
    return jsonify(out[0])


@app.get("/api/title/instruments")
def title_instruments():
    title_no, err = _require_title_no()
    if err:
        return err
    items = _query(f"""
        SELECT
            TIN_ID, INST_NO, INSTRUMENT_CODE, TRT_GRP, TRT_TYPE,
            TRANSACTION_TYPE_DESC, INSTRUMENT_STATUS AS status,
            LODGED_DATETIME, PRIORITY_NO, IS_CURRENT
        FROM L.GOLD.FACT_TITLE_INSTRUMENT
        WHERE TITLE_NO = {_sql_literal(title_no)}
        ORDER BY LODGED_DATETIME DESC NULLS LAST, TIN_ID DESC
        LIMIT 1000
    """)
    return jsonify({"title_no": title_no, "count": len(items), "items": items})


@app.get("/api/title/instruments.csv")
def title_instruments_csv():
    title_no, err = _require_title_no()
    if err:
        return err
    items = _query(f"""
        SELECT
            TIN_ID, INST_NO, INSTRUMENT_CODE, TRT_GRP, TRT_TYPE,
            TRANSACTION_TYPE_DESC, INSTRUMENT_STATUS AS status,
            LODGED_DATETIME, PRIORITY_NO
        FROM L.GOLD.FACT_TITLE_INSTRUMENT
        WHERE TITLE_NO = {_sql_literal(title_no)}
        ORDER BY LODGED_DATETIME DESC NULLS LAST
        LIMIT 20000
    """)
    return _to_csv_response(items, f"linz_title_{title_no}_instruments.csv")


@app.get("/api/title/encumbrances")
def title_encumbrances():
    title_no, err = _require_title_no()
    if err:
        return err
    items = _query(f"""
        SELECT
            TITLE_ENCUMBRANCE_ID, ENC_ID,
            TITLE_ENCUMBRANCE_STATUS, ENCUMBRANCE_STATUS,
            ENCUMBRANCE_TERM, ENCUMBRANCEE_NAME, ENCUMBRANCEE_STATUS,
            IS_CURRENT
        FROM L.GOLD.FACT_TITLE_ENCUMBRANCE
        WHERE TITLE_NO = {_sql_literal(title_no)}
        ORDER BY TITLE_ENCUMBRANCE_ID DESC
        LIMIT 1000
    """)
    return jsonify({"title_no": title_no, "count": len(items), "items": items})


@app.get("/api/title/encumbrances.csv")
def title_encumbrances_csv():
    title_no, err = _require_title_no()
    if err:
        return err
    items = _query(f"""
        SELECT
            TITLE_ENCUMBRANCE_ID, ENC_ID,
            TITLE_ENCUMBRANCE_STATUS, ENCUMBRANCE_STATUS,
            ENCUMBRANCE_TERM, ENCUMBRANCEE_NAME, IS_CURRENT
        FROM L.GOLD.FACT_TITLE_ENCUMBRANCE
        WHERE TITLE_NO = {_sql_literal(title_no)}
        ORDER BY TITLE_ENCUMBRANCE_ID DESC
        LIMIT 20000
    """)
    return _to_csv_response(items, f"linz_title_{title_no}_encumbrances.csv")


@app.get("/api/title/address")
def title_address():
    title_no, err = _require_title_no()
    if err:
        return err
    items = _query(f"""
        SELECT
            ADDRESS_ID, PARCEL_ID, FULL_ADDRESS, STREET_ADDRESS,
            UNIT_TYPE, UNIT_VALUE, STREET_NUMBER, ROAD_NAME, ROAD_TYPE,
            SUBURB, CITY, POSTCODE
        FROM L.GOLD.V_TITLE_ADDRESS
        WHERE TITLE_NO = {_sql_literal(title_no)}
        ORDER BY FULL_ADDRESS
        LIMIT 100
    """)
    return jsonify({"title_no": title_no, "count": len(items), "items": items})


@app.get("/api/title/lineage")
def title_lineage():
    title_no, err = _require_title_no()
    if err:
        return err
    prior = _query(f"""
        SELECT PRIOR_TITLE_NO AS related_title, STATUS, PRIOR_TITLE_STATUS, PRIOR_ISSUE_DATE
        FROM L.GOLD.V_TITLE_LINEAGE
        WHERE FOLLOWING_TITLE_NO = {_sql_literal(title_no)}
        ORDER BY related_title LIMIT 1000
    """)
    follow_on = _query(f"""
        SELECT FOLLOWING_TITLE_NO AS related_title, STATUS, FOLLOWING_TITLE_STATUS, FOLLOWING_ISSUE_DATE
        FROM L.GOLD.V_TITLE_LINEAGE
        WHERE PRIOR_TITLE_NO = {_sql_literal(title_no)}
        ORDER BY related_title LIMIT 1000
    """)
    return jsonify({
        "title_no": title_no,
        "prior_count": len(prior),
        "follow_on_count": len(follow_on),
        "prior": prior,
        "follow_on": follow_on,
    })


@app.get("/api/title/lineage/graph")
def title_lineage_graph():
    title_no, err = _require_title_no()
    if err:
        return err
    prior = _query(f"""
        SELECT PRIOR_TITLE_NO AS related_title FROM L.GOLD.V_TITLE_LINEAGE
        WHERE FOLLOWING_TITLE_NO = {_sql_literal(title_no)} LIMIT 100
    """)
    follow_on = _query(f"""
        SELECT FOLLOWING_TITLE_NO AS related_title FROM L.GOLD.V_TITLE_LINEAGE
        WHERE PRIOR_TITLE_NO = {_sql_literal(title_no)} LIMIT 100
    """)
    nodes = [{"id": title_no, "group": "current"}]
    edges = []
    seen = {title_no}
    for p in prior:
        rid = p["related_title"]
        if rid and rid not in seen:
            nodes.append({"id": rid, "group": "prior"})
            seen.add(rid)
        if rid:
            edges.append({"source": rid, "target": title_no})
    for f in follow_on:
        rid = f["related_title"]
        if rid and rid not in seen:
            nodes.append({"id": rid, "group": "follow"})
            seen.add(rid)
        if rid:
            edges.append({"source": title_no, "target": rid})
    return jsonify({"title_no": title_no, "nodes": nodes, "edges": edges})


@app.get("/api/instruments/search")
def instrument_search():
    q     = (request.args.get("q") or "").strip()
    limit = _to_int(request.args.get("limit"), default=50, min_v=1, max_v=500)
    where = ""
    if q:
        qlit = _sql_literal(f"%{q.lower()}%")
        where = f"""WHERE LOWER(INST_NO) LIKE {qlit}
                   OR LOWER(COALESCE(TRANSACTION_TYPE_DESC, '')) LIKE {qlit}
                   OR LOWER(COALESCE(TRT_TYPE, '')) LIKE {qlit}
                   OR LOWER(COALESCE(TITLE_NO, '')) LIKE {qlit}"""
    items = _query(f"""
        SELECT
            INST_NO, TIN_ID, INSTRUMENT_STATUS AS status, TRT_TYPE,
            TRANSACTION_TYPE_DESC AS transaction_type_description,
            LODGED_DATETIME,
            LISTAGG(DISTINCT TITLE_NO, ',') WITHIN GROUP (ORDER BY TITLE_NO) AS title_nos_str,
            COUNT(DISTINCT TITLE_NO) AS title_count
        FROM L.GOLD.FACT_TITLE_INSTRUMENT
        {where}
        GROUP BY INST_NO, TIN_ID, INSTRUMENT_STATUS, TRT_TYPE, TRANSACTION_TYPE_DESC, LODGED_DATETIME
        ORDER BY LODGED_DATETIME DESC NULLS LAST, TIN_ID DESC
        LIMIT {limit}
    """)
    # split title_nos_str → list for frontend compat
    for item in items:
        s = item.pop("title_nos_str", "") or ""
        item["title_nos"] = [t for t in s.split(",") if t]
    return jsonify({"count": len(items), "items": items})


@app.get("/api/instruments/search.csv")
def instrument_search_csv():
    q     = (request.args.get("q") or "").strip()
    limit = _to_int(request.args.get("limit"), default=5000, min_v=1, max_v=10_000)
    where = ""
    if q:
        qlit = _sql_literal(f"%{q.lower()}%")
        where = f"""WHERE LOWER(INST_NO) LIKE {qlit}
                   OR LOWER(COALESCE(TRANSACTION_TYPE_DESC, '')) LIKE {qlit}
                   OR LOWER(COALESCE(TRT_TYPE, '')) LIKE {qlit}"""
    items = _query(f"""
        SELECT INST_NO, TIN_ID, INSTRUMENT_STATUS AS status, TRT_TYPE,
               TRANSACTION_TYPE_DESC, LODGED_DATETIME,
               COUNT(DISTINCT TITLE_NO) AS title_count
        FROM L.GOLD.FACT_TITLE_INSTRUMENT
        {where}
        GROUP BY INST_NO, TIN_ID, INSTRUMENT_STATUS, TRT_TYPE, TRANSACTION_TYPE_DESC, LODGED_DATETIME
        ORDER BY LODGED_DATETIME DESC NULLS LAST
        LIMIT {limit}
    """)
    return _to_csv_response(items, "linz_instrument_search.csv")


@app.get("/api/instrument")
def instrument_detail():
    inst_no = _parse_title_no((request.args.get("no") or "").strip())
    if inst_no is None:
        return jsonify({"error": "Invalid instrument format"}), 400
    items = _query(f"""
        SELECT
            INST_NO, TIN_ID, INSTRUMENT_STATUS AS status, TRT_GRP, TRT_TYPE,
            TRANSACTION_TYPE_DESC AS transaction_type_description, LODGED_DATETIME,
            LISTAGG(DISTINCT TITLE_NO, ',') WITHIN GROUP (ORDER BY TITLE_NO) AS title_nos_str,
            COUNT(DISTINCT TITLE_NO) AS title_count
        FROM L.GOLD.FACT_TITLE_INSTRUMENT
        WHERE INST_NO = {_sql_literal(inst_no)}
        GROUP BY INST_NO, TIN_ID, INSTRUMENT_STATUS, TRT_GRP, TRT_TYPE,
                 TRANSACTION_TYPE_DESC, LODGED_DATETIME
        LIMIT 1
    """)
    if not items:
        return jsonify({"error": "Not found"}), 404
    item = items[0]
    s = item.pop("title_nos_str", "") or ""
    item["title_nos"] = [t for t in s.split(",") if t]
    return jsonify(item)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
