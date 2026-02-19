from __future__ import annotations

"""
Inventory Management CLI (KiCad BOM -> SQLite -> stock operations)

What this script is for (high-level):
- I export BOM CSVs from KiCad (one CSV per project/pedal).
- I import those BOMs into a local SQLite database.
- I track parts (resistors/caps/diodes/etc.) and my current on-hand stock.
- Then I can:
    * search parts
    * receive stock (increment counts)
    * build a project (deduct BOM quantities from stock)
    * generate a shopping list (what I need to buy to build N units)
    * batch-receive stock from a CSV (useful after a DigiKey/Mouser order)

Design choices I made:
- I normalize part identity into a deterministic "part_key" so matching is consistent:
      part_key = "{prefix}|{value}|{subtype}"
  Example:
      "R|10k|" for a resistor
      "C|100n|film" for a film capacitor
- I keep schema minimal and deterministic so it’s easy to reason about in an interview.
- I treat KiCad BOM as the source of truth for project requirements (qty_per).
"""

import argparse
from pathlib import Path
import sqlite3
import csv
import datetime as dt


# -------------------------
# Helpers (my rules)
# -------------------------

def now_iso() -> str:
    """
    Return current local time as an ISO-like string (no microseconds), e.g.:
      "2026-02-19 08:15:30"
    I store timestamps in the DB in this simple string form for readability.
    """
    return dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")

def norm_space(s: str) -> str:
    """
    Normalize whitespace in a string:
    - strip leading/trailing spaces
    - collapse runs of internal whitespace to single spaces
    This keeps values and footprints consistent across CSVs.
    """
    return " ".join((s or "").strip().split())

def norm_value(s: str) -> str:
    """
    Normalize the 'Value' field from KiCad.
    - If it looks like a simple component value (digits + units, etc.), I lowercase it.
    - I also unify "ohm" -> "Ω" so the same part isn’t duplicated.
    - If it's not a simple value (e.g., descriptive text), I leave it mostly as-is.
    """
    s = norm_space(s)
    if not s:
        return s

    # This character set is my quick heuristic for "simple values" like:
    # 10k, 1u, 100n, 4.7k, 1%, 0.1uF, 22R, etc.
    simple_chars = set("0123456789.kKmMuUnNpPfFrRΩohmOHM%")
    if all(c in simple_chars for c in s):
        return s.lower().replace("ohm", "Ω")

    # If it contains other chars (like full descriptive text),
    # I keep it as-is except whitespace normalization.
    return s

def norm_key(s: str) -> str:
    """
    Normalize part_key for case-insensitive matching.
    The DB stores part_key as a unique key, but I query using LOWER(part_key).
    """
    return (s or "").strip().lower()

def ref_prefix(reference: str) -> str:
    """
    Extract the reference designator prefix from KiCad's "Reference" field.

    Examples:
      "R1" -> "R"
      "C12" -> "C"
      "D3" -> "D"
      "U1" -> "U"
      "RV1" -> "RV"  (anything alphabetical at the start)

    KiCad sometimes formats Reference with comma-separated groupings in BOM outputs,
    so I take only the first group before a comma.
    """
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

    # If I can’t parse anything usable, I fall back to "X" as an unknown prefix.
    return letters or "X"

def cap_subtype_from_footprint(footprint: str) -> str:
    """
    Derive capacitor subtype from footprint naming conventions.

    My deterministic KiCad naming conventions:
      - film:          footprints include "FILM_BOX_Rect"
      - electrolytic:  footprints include "Electro_Radial"

    This lets me distinguish cap types even when their 'Value' is the same.
    """
    fp = (footprint or "").lower()
    if "film_box_rect" in fp:
        return "film"
    if "electro_radial" in fp:
        return "electrolytic"
    return "unknown"

def make_part_key(prefix: str, value: str, subtype: str) -> str:
    """
    Build the canonical part_key string.

    Examples:
      prefix="R", value="10k", subtype=""        -> "R|10k|"
      prefix="C", value="100n", subtype="film"   -> "C|100n|film"
    """
    return f"{prefix}|{value}|{subtype}"


# -------------------------
# DB
# -------------------------

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

-- parts: master list of unique parts
-- part_key is unique; it encodes my identity rules for a part.
CREATE TABLE IF NOT EXISTS parts (
  part_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  part_key  TEXT NOT NULL UNIQUE,
  prefix    TEXT NOT NULL,
  value     TEXT NOT NULL,
  subtype   TEXT NOT NULL DEFAULT '',
  example_footprint TEXT NOT NULL DEFAULT '',
  location  TEXT NOT NULL DEFAULT ''
);

-- stock: quantity tracking for each part
-- one-to-one with parts, keyed by part_id
CREATE TABLE IF NOT EXISTS stock (
  part_id   INTEGER PRIMARY KEY,
  on_hand   INTEGER NOT NULL DEFAULT 0,
  min_stock INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(part_id) REFERENCES parts(part_id) ON DELETE CASCADE
);

-- projects: list of projects (each corresponds to a BOM CSV import)
CREATE TABLE IF NOT EXISTS projects (
  project_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL UNIQUE,
  source_csv TEXT NOT NULL DEFAULT '',
  imported_at TEXT NOT NULL DEFAULT ''
);

-- bom_items: junction table of project -> part with qty_per build
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
    """
    Open a sqlite3 connection and ensure parent directory exists.

    Notes:
    - I set row_factory to sqlite3.Row so I can access columns by name,
      which makes interview discussion clearer than tuple indexing.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con

def init_db(db_path: str) -> None:
    """
    Initialize the database schema if it doesn't exist.
    Safe to call on every command (idempotent).
    """
    con = connect(db_path)
    with con:
        con.executescript(SCHEMA_SQL)
    con.close()

def get_or_create_part(con: sqlite3.Connection, prefix: str, value: str, subtype: str, footprint: str) -> int:
    """
    Ensure a part exists in parts/stock tables and return part_id.

    Logic:
    - Build my canonical part_key.
    - If part exists, return its part_id.
    - Else insert into parts, then ensure a stock row exists (on_hand defaults to 0).
    """
    pkey = make_part_key(prefix, value, subtype)

    # Look for an existing part by unique part_key.
    row = con.execute("SELECT part_id FROM parts WHERE part_key=?", (pkey,)).fetchone()
    if row:
        return int(row["part_id"])

    # Insert new part row.
    cur = con.execute(
        "INSERT INTO parts(part_key, prefix, value, subtype, example_footprint) VALUES(?,?,?,?,?)",
        (pkey, prefix, value, subtype, footprint),
    )
    pid = int(cur.lastrowid)

    # Ensure stock row exists for this part. Using OR IGNORE protects against duplicates.
    con.execute("INSERT OR IGNORE INTO stock(part_id, on_hand, min_stock) VALUES(?,0,0)", (pid,))
    return pid


# -------------------------
# BOM reading + import
# -------------------------

def read_kicad_bom(csv_path: str):
    """
    Read a KiCad BOM CSV and return normalized rows:
        (reference, value, footprint, qty)

    Assumptions:
    - The BOM CSV includes columns: Reference, Value, Footprint, Qty, DNP.
    - DNP rules:
        * If DNP is non-empty and not in (0, false, no, n, ""), I skip the row.
          This allows KiCad exports where DNP is set to "1" or "true" etc.

    Data hygiene:
    - I normalize whitespace on Reference and Footprint.
    - I normalize Value using my 'norm_value' rules.
    - I coerce Qty to an integer (handles BOMs where Qty is "1.0").
    """
    rows = []
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)

        required = {"Reference", "Value", "Footprint", "Qty", "DNP"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise ValueError(f"{csv_path}: missing columns: {sorted(missing)}")

        for row in r:
            # DNP filtering: if DNP is set to something truthy (like "1" or "TRUE"), skip it.
            dnp = (row.get("DNP") or "").strip()
            if dnp and dnp.lower() not in ("0", "false", "no", "n", ""):
                continue

            ref = norm_space(row.get("Reference", ""))
            val = norm_value(row.get("Value", ""))
            fp  = norm_space(row.get("Footprint", ""))

            # Qty must be present and positive.
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
    """
    Import a single project's BOM into the database.

    Strategy:
    - Parse and normalize the BOM CSV.
    - Upsert the project row (projects table) with source path + timestamp.
    - Delete any existing bom_items for that project.
    - For each BOM entry:
        * derive prefix from Reference (R/C/D/U/etc.)
        * for capacitors only, derive subtype from footprint
        * get_or_create_part(...) to ensure the part exists
        * insert bom_items(project_id, part_id, qty_per)

    Result:
    - The DB now knows how many of each part is required per build of that project.
    """
    bom = read_kicad_bom(csv_path)

    with con:
        # Upsert project record (keeps project name stable; updates source path and timestamp on re-import).
        con.execute(
            "INSERT INTO projects(name, source_csv, imported_at) VALUES(?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET source_csv=excluded.source_csv, imported_at=excluded.imported_at",
            (project, str(Path(csv_path).resolve()), now_iso()),
        )

        # Fetch project_id so I can load bom_items.
        proj_id = int(con.execute("SELECT project_id FROM projects WHERE name=?", (project,)).fetchone()["project_id"])

        # I treat the BOM import as authoritative, so I replace prior BOM items each time.
        con.execute("DELETE FROM bom_items WHERE project_id=?", (proj_id,))

        for ref, val, fp, qty in bom:
            prefix = ref_prefix(ref)

            # Only capacitors get subtype classification; other prefixes keep subtype empty for now.
            subtype = cap_subtype_from_footprint(fp) if prefix == "C" else ""

            pid = get_or_create_part(con, prefix, val, subtype, fp)

            # Insert required quantity per build for this project.
            con.execute(
                "INSERT INTO bom_items(project_id, part_id, qty_per) VALUES(?,?,?)",
                (proj_id, pid, qty),
            )


# -------------------------
# Output formatting
# -------------------------

def print_table(rows, headers):
    """
    Simple console table formatter for readable CLI output.

    - Computes column widths based on headers and cell contents
    - Prints a header row, separator row, then data rows

    I kept this in pure Python (no external deps) so it runs anywhere.
    """
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
    """
    Import all BOM CSVs from a folder.

    Behavior:
    - The project name is derived from the filename stem (lowercased).
      Example: "Seaholm.csv" -> project "seaholm"
    - Each CSV is imported with the same rules as import_bom().
    """
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
    """
    Search parts across key fields.

    Search target fields:
    - part_key (my canonical identity)
    - value (e.g., 10k, 100n)
    - subtype (film/electrolytic/unknown for caps)
    - prefix (R/C/D/U/etc.)
    - location (my physical storage label)

    Implementation detail:
    - I use LIKE with a lowercased query for simple case-insensitive partial matching.
    - I cap results at 200 for usability.
    """
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
    """
    Receive (add) inventory for a given part_key.

    Inputs:
    - part_key: must match an existing parts.part_key (case-insensitive)
    - qty: integer quantity to add
    - --loc: optional physical location update (bin label, drawer, etc.)

    Behavior:
    - If part_key is unknown, I fail fast.
    - Otherwise I add qty to stock.on_hand.
    - If location is provided, I also update parts.location.
    """
    init_db(args.db)
    con = connect(args.db)

    part_key = args.part_key
    qty = int(args.qty)

    # Case-insensitive lookup by part_key.
    row = con.execute(
        "SELECT part_id FROM parts WHERE LOWER(part_key)=?",
        (norm_key(part_key),)
    ).fetchone()

    if not row:
        raise SystemExit(f"Unknown part_key: {part_key}")

    pid = int(row["part_id"])

    with con:
        # Increment stock level.
        con.execute(
            "UPDATE stock SET on_hand = on_hand + ? WHERE part_id=?",
            (qty, pid),
        )

        # Optionally update storage location label.
        if args.loc:
            con.execute("UPDATE parts SET location=? WHERE part_id=?", (args.loc, pid))

    print(f"Added {qty} to {part_key}")
    con.close()

def cmd_build(args):
    """
    Build N units of a project and deduct inventory accordingly.

    Inputs:
    - project: name of project (from imported BOM)
    - qty: how many builds to perform
    - --force: allow inventory to go negative

    Process:
    - Load all BOM items for that project.
    - For each part:
        need = qty_per * build_qty
        new_have = on_hand - need
    - If any would go negative and --force is not set:
        * print a shortage table
        * exit with code 2 (so scripts/CI can detect failure)
    - Else:
        * deduct all needs from stock in a transaction

    Important:
    - I compute shortages first before applying deductions so I can fail safely.
    - When --force is used, I still show shortages so I know what I owe myself later.
    """
    init_db(args.db)
    con = connect(args.db)

    project = args.project.lower()
    build_qty = int(args.qty)

    # Verify project exists.
    proj = con.execute("SELECT project_id FROM projects WHERE name=?", (project,)).fetchone()
    if not proj:
        raise SystemExit(f"Unknown project: {project} (did you import BOMs?)")

    proj_id = int(proj["project_id"])

    # Pull BOM items + current stock levels.
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
    deductions = []  # list of (part_id, need)

    # Pre-check pass: compute needs and detect negative outcomes.
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
                -new_have  # short_by
            ))

    # If shortages exist and I'm not forcing, I abort without touching inventory.
    if shortages and not args.force:
        print("Build would cause negative inventory. Shortages:")
        print_table(shortages, ["part_key", "value", "subtype", "needed", "on_hand", "short_by"])
        con.close()
        raise SystemExit(2)

    # Apply deductions as a single transaction.
    with con:
        for part_id, need in deductions:
            con.execute(
                "UPDATE stock SET on_hand = on_hand - ? WHERE part_id=?",
                (need, part_id),
            )

    con.close()
    print(f"Built {build_qty}x {project} (inventory deducted).")

    # If forced, I still surface what went negative so I can reorder.
    if shortages:
        print("\n⚠ Shortages (inventory is now negative due to --force):")
        print_table(shortages, ["part_key", "value", "subtype", "needed", "on_hand", "short_by"])

def cmd_shop(args):
    """
    Generate a shopping list for building N units of a project.

    Key difference vs build:
    - This does NOT modify inventory.

    For each BOM item:
      need = qty_per * build_qty
      to_order = max(0, need - on_hand)

    Output:
    - Only parts where to_order > 0 are listed.
    """
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
        to_order = max(0, need - have)
        if to_order > 0:
            out.append((it["part_key"], it["value"], it["subtype"], need, have, to_order))

    con.close()

    print(f"Shopping list for {build_qty}x {project} (does not modify inventory):")
    print_table(out, ["part_key", "value", "subtype", "needed", "on_hand", "to_order"])

def cmd_receive_csv(args):
    """
    Batch receive inventory from a CSV file.

    Expected input CSV columns:
      - part_key
      - qty
      - location (optional)

    Behavior:
    - For each row, I look up part_id by part_key (case-insensitive).
    - If unknown, I skip it and print a warning (keeps batch jobs resilient).
    - If known:
        * add qty to stock.on_hand
        * update parts.location if provided
    """
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
            row = con.execute(
                "SELECT part_id FROM parts WHERE LOWER(part_key)=?",
                (norm_key(pkey),)
            ).fetchone()

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
    """
    Build the argparse CLI interface.

    Commands:
    - import-many: import all BOM CSVs in a folder
    - search:      search parts
    - receive:     add inventory for a part_key
    - build:       deduct inventory for a project build
    - shop:        compute shopping list for a build
    - receive-csv: batch receive inventory from a CSV
    """
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
    """
    Entry point:
    - parse CLI arguments
    - dispatch to the selected command handler
    """
    args = build_parser().parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
