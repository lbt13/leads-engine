import sys
import sqlite3
import csv
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

DB = ROOT / "leads.db"

if not DB.exists():
    print("Aucune base de donnees trouvee. Lance d'abord main.py.")
    sys.exit(1)

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT * FROM leads ORDER BY google_rating DESC").fetchall()
conn.close()

if not rows:
    print("Aucun lead en base.")
    sys.exit(0)

# ── Export CSV ────────────────────────────────────────────
csv_path = ROOT / "leads_export.csv"
with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows([dict(r) for r in rows])

print(f"\n  {len(rows)} leads exportes dans : {csv_path}")
print(f"\n  {'Entreprise':<35} {'Ville':<15} {'Note':>5}  {'Site'}")
print(f"  {'-'*75}")
for r in rows:
    site = r['website_url'][:30] if r['website_url'] else '—'
    note = f"{r['google_rating']:.1f}" if r['google_rating'] else '—'
    print(f"  {r['company_name'][:34]:<35} {(r['city'] or '')[:14]:<15} {note:>5}  {site}")
print(f"\n  Ouvre leads_export.csv avec Excel\n")