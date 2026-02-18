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
    
def norm_key(s: str) -> str:
    """Normalize part_key for case-insensitive matching."""
    return (s or "").strip().lower()

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

    q = (args.query or "").strip()

    sql = """
        SELECT p.part_key, p.value, p.prefix, p.subtype, s.on_hand, p.location
        FROM parts p
        JOIN stock s ON s.part_id = p.part_id
        WHERE LOWER(p.part_key) LIKE ?
           OR LOWER(p.value) LIKE ?
           OR LOWER(p.subtype) LIKE ?
           OR LOWER(p.prefix) LIKE ?
           OR LOWER(p.location) LIKE ?
        ORDER BY p.prefix, p.value, p.subtype
        LIMIT 200
    """

    like = f"%{q.lower()}%"
    rows = con.execute(sql, (like, like, like, like, like)).fetchall()

    con.close()
    print_table([tuple(r) for r in rows], ["part_key", "value", "prefix", "subtype", "on_hand", "location"])

def cmd_receive(args):
    init_db(args.db)
    con = connect(args.db)

    part_key = args.part_key
    qty = int(args.qty)

    row = con.execute(
    "SELECT part_id FROM parts WHERE LOWER(part_key)=?",(norm_key(part_key),)).fetchone()

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

def cmd_build(args):
    init_db(args.db)
    con = connect(args.db)

    project = args.project.lower()
    build_qty = int(args.qty)

    proj = con.execute("SELECT project_id FROM projects WHERE name=?", (project,)).fetchone()
    if not proj:
        raise SystemExit(f"Unknown project: {project} (did you import BOMs?)")

    proj_id = int(proj["project_id"])

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

    if not items:
        raise SystemExit(f"Project '{project}' has no BOM items.")

    shortages = []
    deductions = []  # (part_id, need)

    for it in items:
        need = int(it["qty_per"]) * build_qty
        have = int(it["on_hand"])
        new_have = have - need

        deductions.append((int(it["part_id"]), need))

        if new_have < 0:
            shortages.append((
                it["part_key"],
                it["value"],
                it["subtype"],
                need,
                have,
                -new_have
            ))

    if shortages and not args.force:
        print("Build would cause negative inventory. Shortages:")
        print_table(shortages, ["part_key", "value", "subtype", "needed", "on_hand", "short_by"])
        con.close()
        raise SystemExit(2)

    # Apply deductions
    with con:
        for part_id, need in deductions:
            con.execute(
                "UPDATE stock SET on_hand = on_hand - ? WHERE part_id=?",
                (need, part_id),
            )

    con.close()
    print(f"Built {build_qty}x {project} (inventory deducted).")

    if shortages:
        print("\n⚠ Shortages (inventory is now negative due to --force):")
        print_table(shortages, ["part_key", "value", "subtype", "needed", "on_hand", "short_by"])

def cmd_shop(args):
    init_db(args.db)
    con = connect(args.db)

    project = args.project.lower()
    build_qty = int(args.qty)

    proj = con.execute("SELECT project_id FROM projects WHERE name=?", (project,)).fetchone()
    if not proj:
        raise SystemExit(f"Unknown project: {project}")

    proj_id = int(proj["project_id"])

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
        need = int(it["qty_per"]) * build_qty
        have = int(it["on_hand"])
        # how many to buy to be able to build (need - have), but only if short
        to_order = max(0, need - have)
        if to_order > 0:
            out.append((it["part_key"], it["value"], it["subtype"], need, have, to_order))

    con.close()

    print(f"Shopping list for {build_qty}x {project} (does not modify inventory):")
    print_table(out, ["part_key", "value", "subtype", "needed", "on_hand", "to_order"])

def cmd_receive_csv(args):
    init_db(args.db)
    con = connect(args.db)

    path = Path(args.csv)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"part_key", "qty"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise SystemExit(f"CSV missing columns: {missing}")

        for row in r:
            pkey = row["part_key"].strip()
            qty = int(row["qty"])
            loc = (row.get("location") or "").strip()
            rows.append((pkey, qty, loc))

    with con:
        for pkey, qty, loc in rows:
            row = con.execute("SELECT part_id FROM parts WHERE LOWER(part_key)=?",(norm_key(pkey),)).fetchone()

            if not row:
                print(f"⚠ Skipping unknown part_key: {pkey}")
                continue

            pid = int(row["part_id"])
            con.execute("UPDATE stock SET on_hand = on_hand + ? WHERE part_id=?", (qty, pid))

            if loc:
                con.execute("UPDATE parts SET location=? WHERE part_id=?", (loc, pid))

    con.close()
    print(f"Batch received {len(rows)} items from {path}")



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

    s = sp.add_parser("receive", help="Add inventory for a part_key.")
    s.add_argument("part_key")
    s.add_argument("qty", type=int)
    s.add_argument("--loc", help="Storage location label")
    s.set_defaults(func=cmd_receive)

    s = sp.add_parser("build", help="Deduct inventory based on a project BOM x build quantity.")
    s.add_argument("project", help="Project name (e.g., seaholm)")
    s.add_argument("qty", type=int, help="How many pedals to build")
    s.add_argument("--force", action="store_true", help="Allow inventory to go negative")
    s.set_defaults(func=cmd_build)

    s = sp.add_parser("shop", help="Show what to order for a build (does not change inventory).")
    s.add_argument("project")
    s.add_argument("qty", type=int)
    s.set_defaults(func=cmd_shop)

    s = sp.add_parser("receive-csv", help="Batch receive inventory from a CSV file.")
    s.add_argument("csv", help="CSV with columns: part_key,qty,location(optional)")
    s.set_defaults(func=cmd_receive_csv)


    return p

def main():
    args = build_parser().parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
