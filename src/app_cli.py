from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3
import csv
import datetime as dt


# -------------------------
# Helpers (your rules)
# -------------------------

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
    # Your deterministic KiCad naming:
    # film: FILM_BOX_Rect
    # electrolytic: Electro_Radial
    fp = (footprint or "").lower()
    if "film_box_rect" in fp:
        return "film"
    if "electro_radial" in fp:
        return "electrolytic"
    return "unknown"

def make_part_key(prefix: str, value: str, subtype: str) -> str:
    return f"{prefix}|{value}|{subtype}"


# -------------------------
# DB
# -------------------------

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

def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con

def init_db(db_path: str) -> None:
    con = connect(db_path)
    with con:
        con.executescript(SCHEMA_SQL)
    con.close()

def get_or_create_part(con: sqlite3.Connection, prefix: str, value: str, subtype: str, footprint: str) -> int:
    pkey = make_part_key(prefix, value, subtype)
    row = con.execute("SELECT part_id FROM parts WHERE part_key=?", (pkey,)).fetchone()
    if row:
        return int(row["part_id"])
    cur = con.execute(
        "INSERT INTO parts(part_key, prefix, value, subtype, example_footprint) VALUES(?,?,?,?,?)",
        (pkey, prefix, value, subtype, footprint),
    )
    pid = int(cur.lastrowid)
    con.execute("INSERT OR IGNORE INTO stock(part_id, on_hand, min_stock) VALUES(?,0,0)", (pid,))
    return pid


# -------------------------
# BOM reading + import
# -------------------------

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

def import_bom(con: sqlite3.Connection, project: str, csv_path: str) -> None:
    bom = read_kicad_bom(csv_path)

    with con:
        con.execute(
            "INSERT INTO projects(name, source_csv, imported_at) VALUES(?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET source_csv=excluded.source_csv, imported_at=excluded.imported_at",
            (project, str(Path(csv_path).resolve()), now_iso()),
        )
        proj_id = int(con.execute("SELECT project_id FROM projects WHERE name=?", (project,)).fetchone()["project_id"])
        con.execute("DELETE FROM bom_items WHERE project_id=?", (proj_id,))

        for ref, val, fp, qty in bom:
            prefix = ref_prefix(ref)
            subtype = cap_subtype_from_footprint(fp) if prefix == "C" else ""
            pid = get_or_create_part(con, prefix, val, subtype, fp)
            con.execute(
                "INSERT INTO bom_items(project_id, part_id, qty_per) VALUES(?,?,?)",
                (proj_id, pid, qty),
            )


# -------------------------
# Output formatting
# -------------------------

def print_table(rows, headers):
    if not rows:
        print("(no results)")
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * w for w in widths]))
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))


# -------------------------
# CLI Commands
# -------------------------

def cmd_import_many(args):
    init_db(args.db)
    con = connect(args.db)

    boms_dir = Path(args.boms)
    csvs = sorted(boms_dir.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"No CSVs found in {boms_dir}")

    for csv_path in csvs:
        project = csv_path.stem.lower()
        import_bom(con, project, str(csv_path))
        print(f"Imported {project} from {csv_path.name}")

    con.close()

def cmd_search(args):
    init_db(args.db)
    con = connect(args.db)

    q = args.query.strip()
    rows = con.execute(
        """
        SELECT p.part_key, p.value, p.prefix, p.subtype, s.on_hand, p.location
        FROM parts p
        JOIN stock s ON s.part_id = p.part_id
        WHERE p.part_key LIKE ?
           OR p.value LIKE ?
           OR p.subtype LIKE ?
           OR p.prefix LIKE ?
           OR p.location LIKE ?
        ORDER BY p.prefix, p.value, p.subtype
        LIMIT 200
        """,
        (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"),
    ).fetchall()

    con.close()
    print_table([tuple(r) for r in rows], ["part_key", "value", "prefix", "subtype", "on_hand", "location"])

def cmd_receive(args):
    init_db(args.db)
    con = connect(args.db)

    part_key = args.part_key
    qty = int(args.qty)

    row = con.execute("SELECT part_id FROM parts WHERE part_key=?", (part_key,)).fetchone()
    if not row:
        raise SystemExit(f"Unknown part_key: {part_key}")

    pid = int(row["part_id"])

    with con:
        con.execute(
            "UPDATE stock SET on_hand = on_hand + ? WHERE part_id=?",
            (qty, pid),
        )

        if args.loc:
            con.execute("UPDATE parts SET location=? WHERE part_id=?", (args.loc, pid))

    print(f"Added {qty} to {part_key}")
    con.close()

def build_parser():
    p = argparse.ArgumentParser(description="Inventory Mgmt (Step 1: import BOMs + search).")
    p.add_argument("--db", default="./data/inventory.db", help="SQLite DB path")

    sp = p.add_subparsers(dest="cmd", required=True)

    s = sp.add_parser("import-many", help="Import all KiCad BOM CSVs from a folder (project name = filename).")
    s.add_argument("--boms", default="./boms", help="Folder containing BOM CSVs")
    s.set_defaults(func=cmd_import_many)

    s = sp.add_parser("search", help="Search parts (value, part_key, subtype, prefix, location).")
    s.add_argument("query")
    s.set_defaults(func=cmd_search)

    return p

def main():
    args = build_parser().parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
