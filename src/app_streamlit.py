import sqlite3
import csv
import io
from pathlib import Path

import streamlit as st
import datetime as dt

# ----------------------------
# DB schema (same as your CLI)
# ----------------------------
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS parts (
  part_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  part_key  TEXT NOT NULL UNIQUE,
  prefix    TEXT NOT NULL,
  value     TEXT NOT NULL,
  subtype   TEXT NOT NULL DEFAULT '',
  example_footprint TEXT NOT NULL DEFAULT '',
  location  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS stock (
  part_id   INTEGER PRIMARY KEY,
  on_hand   INTEGER NOT NULL DEFAULT 0,
  min_stock INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(part_id) REFERENCES parts(part_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS projects (
  project_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL UNIQUE,
  source_csv TEXT NOT NULL DEFAULT '',
  imported_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS bom_items (
  project_id INTEGER NOT NULL,
  part_id    INTEGER NOT NULL,
  qty_per    INTEGER NOT NULL,
  PRIMARY KEY(project_id, part_id),
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  FOREIGN KEY(part_id) REFERENCES parts(part_id) ON DELETE CASCADE
);
"""

import datetime as dt

def now_iso() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")

def norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())

def norm_value(s: str) -> str:
    s = norm_space(s)
    if not s:
        return s
    simple_chars = set("0123456789.kKmMuUnNpPfFrRΩohmOHM%")
    if all(c in simple_chars for c in s):
        return s.lower().replace("ohm", "Ω")
    return s

def ref_prefix(reference: str) -> str:
    r = (reference or "").strip()
    if not r:
        return "X"
    first = r.split(",")[0].strip()
    letters = ""
    for ch in first:
        if ch.isalpha():
            letters += ch.upper()
        else:
            break
    return letters or "X"

def cap_subtype_from_footprint(footprint: str) -> str:
    fp = (footprint or "").lower()
    if "film_box_rect" in fp:
        return "film"
    if "electro_radial" in fp:
        return "electrolytic"
    return "unknown"

def make_part_key(prefix: str, value: str, subtype: str) -> str:
    return f"{prefix}|{value}|{subtype}"

def read_kicad_bom(csv_path: str):
    rows = []
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        required = {"Reference", "Value", "Footprint", "Qty", "DNP"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise ValueError(f"{csv_path}: missing columns: {sorted(missing)}")

        for row in r:
            dnp = (row.get("DNP") or "").strip()
            if dnp and dnp.lower() not in ("0", "false", "no", "n", ""):
                continue

            ref = norm_space(row.get("Reference", ""))
            val = norm_value(row.get("Value", ""))
            fp  = norm_space(row.get("Footprint", ""))

            qty_raw = (row.get("Qty") or "").strip()
            if not qty_raw:
                continue
            try:
                qty = int(float(qty_raw))
            except ValueError:
                continue
            if qty <= 0:
                continue

            rows.append((ref, val, fp, qty))
    return rows

def get_or_create_part(con, prefix: str, value: str, subtype: str, footprint: str) -> int:
    pkey = make_part_key(prefix, value, subtype)
    row = con.execute("SELECT part_id FROM parts WHERE part_key=?", (pkey,)).fetchone()
    if row:
        return int(row["part_id"]) if isinstance(row, sqlite3.Row) else int(row[0])

    cur = con.execute(
        "INSERT INTO parts(part_key, prefix, value, subtype, example_footprint) VALUES(?,?,?,?,?)",
        (pkey, prefix, value, subtype, footprint),
    )
    pid = int(cur.lastrowid)
    con.execute("INSERT OR IGNORE INTO stock(part_id, on_hand, min_stock) VALUES(?,0,0)", (pid,))
    return pid

def import_bom_into_db(con, project: str, csv_path: str) -> None:
    bom = read_kicad_bom(csv_path)

    with con:
        con.execute(
            "INSERT INTO projects(name, source_csv, imported_at) VALUES(?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET source_csv=excluded.source_csv, imported_at=excluded.imported_at",
            (project, str(Path(csv_path).resolve()), now_iso()),
        )
        proj_id = con.execute("SELECT project_id FROM projects WHERE name=?", (project,)).fetchone()
        proj_id = int(proj_id["project_id"]) if isinstance(proj_id, sqlite3.Row) else int(proj_id[0])

        con.execute("DELETE FROM bom_items WHERE project_id=?", (proj_id,))

        for ref, val, fp, qty in bom:
            prefix = ref_prefix(ref)
            subtype = cap_subtype_from_footprint(fp) if prefix == "C" else ""
            pid = get_or_create_part(con, prefix, val, subtype, fp)
            con.execute(
                "INSERT INTO bom_items(project_id, part_id, qty_per) VALUES(?,?,?)",
                (proj_id, pid, qty),
            )

def auto_import_boms_if_needed(con, boms_dir: str = "boms") -> dict:
    """
    If there are no projects in DB, import all *.csv in boms_dir.
    Returns a small status dict for logging/UI.
    """
    count = con.execute("SELECT COUNT(*) FROM projects").fetchone()
    count = int(count[0]) if not isinstance(count, sqlite3.Row) else int(list(count)[0])
    if count > 0:
        return {"imported": 0, "reason": "projects already exist"}

    p = Path(boms_dir)
    if not p.exists():
        return {"imported": 0, "reason": f"boms_dir not found: {boms_dir}"}

    csvs = sorted(p.glob("*.csv"))
    imported = 0
    for csv_file in csvs:
        import_bom_into_db(con, csv_file.stem.lower(), str(csv_file))
        imported += 1

    return {"imported": imported, "reason": f"imported {imported} BOM(s) from {boms_dir}"}

def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    with con:
        con.executescript(SCHEMA_SQL)
    con.close()


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def like(s: str) -> str:
    return f"%{(s or '').strip().lower()}%"


def search_parts(con: sqlite3.Connection, q: str, limit: int = 300):
    sql = """
        SELECT p.part_key, p.value, p.prefix, p.subtype, s.on_hand, s.min_stock, p.location
        FROM parts p
        JOIN stock s ON s.part_id = p.part_id
        WHERE LOWER(p.part_key) LIKE ?
           OR LOWER(p.value) LIKE ?
           OR LOWER(p.subtype) LIKE ?
           OR LOWER(p.prefix) LIKE ?
           OR LOWER(p.location) LIKE ?
        ORDER BY p.prefix, p.value, p.subtype
        LIMIT ?
    """
    l = like(q)
    return con.execute(sql, (l, l, l, l, l, limit)).fetchall()


def list_projects(con: sqlite3.Connection):
    rows = con.execute("SELECT name, source_csv, imported_at FROM projects ORDER BY name").fetchall()
    return rows


def receive_one(con: sqlite3.Connection, part_key: str, qty: int, location: str = "") -> tuple[bool, str]:
    if qty <= 0:
        return False, "Quantity must be > 0"

    key_norm = (part_key or "").strip().lower()
    row = con.execute(
        "SELECT part_id, part_key FROM parts WHERE LOWER(part_key)=?",
        (key_norm,),
    ).fetchone()

    if not row:
        return False, f"Unknown part_key: {part_key}"

    pid = int(row["part_id"])
    canonical = row["part_key"]

    with con:
        con.execute("UPDATE stock SET on_hand = on_hand + ? WHERE part_id=?", (qty, pid))
        if location.strip():
            con.execute("UPDATE parts SET location=? WHERE part_id=?", (location.strip(), pid))

    return True, f"Added {qty} to {canonical}"


def receive_csv(con: sqlite3.Connection, csv_text: str) -> dict:
    """
    CSV columns required: part_key, qty
    Optional: location
    Case-insensitive matching on part_key.
    """
    f = io.StringIO(csv_text)
    r = csv.DictReader(f)

    required = {"part_key", "qty"}
    if not r.fieldnames or not required.issubset(set(r.fieldnames)):
        return {"ok": False, "msg": f"CSV must include columns: {sorted(required)} (optional: location)"}

    applied = 0
    skipped = []
    matched = []

    with con:
        for row in r:
            pkey = (row.get("part_key") or "").strip()
            qty_raw = (row.get("qty") or "").strip()
            loc = (row.get("location") or "").strip()

            if not pkey or not qty_raw:
                continue

            try:
                qty = int(float(qty_raw))
            except ValueError:
                skipped.append((pkey, qty_raw, "qty not an integer"))
                continue

            if qty == 0:
                continue

            dbrow = con.execute(
                "SELECT part_id, part_key FROM parts WHERE LOWER(part_key)=?",
                (pkey.lower(),),
            ).fetchone()

            if not dbrow:
                skipped.append((pkey, qty, "unknown part_key"))
                continue

            pid = int(dbrow["part_id"])
            canonical = dbrow["part_key"]

            con.execute("UPDATE stock SET on_hand = on_hand + ? WHERE part_id=?", (qty, pid))
            if loc:
                con.execute("UPDATE parts SET location=? WHERE part_id=?", (loc, pid))

            applied += 1
            if canonical.lower() != pkey.lower():
                matched.append((pkey, canonical))

    return {"ok": True, "applied": applied, "skipped": skipped, "matched": matched}


def shop_for_build(con: sqlite3.Connection, project: str, qty: int):
    project = (project or "").strip().lower()
    if qty <= 0:
        return None, "Build quantity must be > 0"

    proj = con.execute("SELECT project_id, name FROM projects WHERE LOWER(name)=?", (project,)).fetchone()
    if not proj:
        return None, f"Unknown project: {project}"

    proj_id = int(proj["project_id"])
    name = proj["name"]

    items = con.execute(
        """
        SELECT
          p.part_key, p.value, p.subtype,
          b.qty_per,
          s.on_hand
        FROM bom_items b
        JOIN parts p ON p.part_id = b.part_id
        JOIN stock s ON s.part_id = p.part_id
        WHERE b.project_id = ?
        ORDER BY p.prefix, p.value, p.subtype
        """,
        (proj_id,),
    ).fetchall()

    out = []
    for it in items:
        need = int(it["qty_per"]) * qty
        have = int(it["on_hand"])
        to_order = max(0, need - have)
        if to_order > 0:
            out.append({
                "part_key": it["part_key"],
                "value": it["value"],
                "subtype": it["subtype"],
                "needed": need,
                "on_hand": have,
                "to_order": to_order,
            })

    return {"project": name, "qty": qty, "rows": out}, None


def build_project(con: sqlite3.Connection, project: str, qty: int, force: bool):
    """
    Deduct inventory based on BOM qty_per * qty.
    If force=False and any part would go negative, do NOT deduct and return shortages.
    """
    project = (project or "").strip().lower()
    if qty <= 0:
        return None, "Build quantity must be > 0"

    proj = con.execute("SELECT project_id, name FROM projects WHERE LOWER(name)=?", (project,)).fetchone()
    if not proj:
        return None, f"Unknown project: {project}"

    proj_id = int(proj["project_id"])
    name = proj["name"]

    items = con.execute(
        """
        SELECT
          p.part_id,
          p.part_key,
          p.value,
          p.subtype,
          b.qty_per,
          s.on_hand
        FROM bom_items b
        JOIN parts p ON p.part_id = b.part_id
        JOIN stock s ON s.part_id = p.part_id
        WHERE b.project_id = ?
        ORDER BY p.prefix, p.value, p.subtype
        """,
        (proj_id,),
    ).fetchall()

    shortages = []
    deductions = []

    for it in items:
        need = int(it["qty_per"]) * qty
        have = int(it["on_hand"])
        new_have = have - need
        deductions.append((int(it["part_id"]), need))

        if new_have < 0:
            shortages.append({
                "part_key": it["part_key"],
                "value": it["value"],
                "subtype": it["subtype"],
                "needed": need,
                "on_hand": have,
                "short_by": -new_have,
            })

    if shortages and not force:
        return {"project": name, "qty": qty, "shortages": shortages, "deducted": False}, None

    with con:
        for part_id, need in deductions:
            con.execute("UPDATE stock SET on_hand = on_hand - ? WHERE part_id=?", (need, part_id))

    return {"project": name, "qty": qty, "shortages": shortages, "deducted": True}, None


# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Inventory Mgmt", layout="wide")
st.title("Inventory Management (KiCad BOM → Stock)")

db_path = st.sidebar.text_input("DB path", "./data/inventory.db")
init_db(db_path)

con = connect(db_path)

status = auto_import_boms_if_needed(con, boms_dir="boms")
# Optional: show in sidebar while debugging
st.sidebar.caption(f"BOM auto-import: {status['reason']}")


projects = list_projects(con)
project_names = [p["name"] for p in projects]

with st.sidebar:
    st.subheader("Projects")
    if project_names:
        st.write(project_names)
    else:
        st.info("No projects found. Import BOMs via CLI first.")

tab_search, tab_receive, tab_receive_csv, tab_shop, tab_build = st.tabs(
    ["Search", "Receive", "Receive CSV", "Shop", "Build"]
)

with tab_search:
    q = st.text_input("Search (value / part_key / subtype / location)", "")
    if q:
        rows = search_parts(con, q)
        st.dataframe([dict(r) for r in rows], use_container_width=True, hide_index=True)
    else:
        st.caption("Try: 100n, film, electro, TL072, 1n5817, B100K, etc.")

with tab_receive:
    col1, col2, col3 = st.columns([2, 1, 2])
    with col1:
        pkey = st.text_input("part_key (case-insensitive)", placeholder='e.g. C|100n|film')
    with col2:
        qty = st.number_input("Qty", min_value=1, step=1, value=1)
    with col3:
        loc = st.text_input("Location (optional)", placeholder="e.g. Film Caps Bin")

    if st.button("Receive"):
        ok, msg = receive_one(con, pkey, int(qty), loc)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

with tab_receive_csv:
    st.write("Upload a CSV with columns: **part_key, qty** (optional: location). Matching is case-insensitive.")
    up = st.file_uploader("CSV file", type=["csv"])

    sample = "part_key,qty,location\nC|100N|FILM,100,Film Caps\nD|1N5817|,50,Diodes\nR|10K|,500,Res Drawer\n"
    st.download_button("Download sample CSV", sample, file_name="receiving_sample.csv")

    if up is not None:
        text = up.getvalue().decode("utf-8", errors="replace")
        result = receive_csv(con, text)

        if not result.get("ok"):
            st.error(result["msg"])
        else:
            st.success(f"Applied {result['applied']} rows.")
            if result["matched"]:
                st.info("Case-insensitive matches (input → canonical):")
                st.dataframe(result["matched"], use_container_width=True, hide_index=True)
            if result["skipped"]:
                st.warning("Skipped rows:")
                st.dataframe(result["skipped"], use_container_width=True, hide_index=True)

with tab_shop:
    col1, col2 = st.columns([2, 1])
    with col1:
        proj = st.selectbox("Project", options=project_names) if project_names else st.text_input("Project")
    with col2:
        n = st.number_input("Quantity", min_value=1, step=1, value=1)

    if st.button("Generate shopping list"):
        data, err = shop_for_build(con, proj, int(n))
        if err:
            st.error(err)
        else:
            st.success(f"Shopping list for {data['qty']}x {data['project']} (no inventory changes).")
            st.dataframe(data["rows"], use_container_width=True, hide_index=True)

with tab_build:
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        proj = st.selectbox("Project", options=project_names, key="build_project") if project_names else st.text_input("Project", key="build_project")
    with col2:
        n = st.number_input("Quantity", min_value=1, step=1, value=1, key="build_qty")
    with col3:
        force = st.checkbox("Force (allow negatives)", value=False)

    if st.button("Build (deduct inventory)"):
        result, err = build_project(con, proj, int(n), force=force)
        if err:
            st.error(err)
        else:
            if not result["deducted"]:
                st.error("Build blocked — shortages would occur. Enable Force to proceed.")
            else:
                st.success(f"Built {result['qty']}x {result['project']} (inventory deducted).")

            if result["shortages"]:
                st.warning("Shortages:")
                st.dataframe(result["shortages"], use_container_width=True, hide_index=True)
            else:
                st.info("No shortages.")

con.close()
