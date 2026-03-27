import sys, sqlite3, logging
from pathlib import Path
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parent))

from core.queue import LeadQueue
from agents.extractor import process_lead

conn = sqlite3.connect("leads.db")
conn.row_factory = sqlite3.Row

# Affiche les colonnes existantes
cols = [r[1] for r in conn.execute("PRAGMA table_info(leads)").fetchall()]
print("\nColonnes en base :")
for c in cols: print(f"  {c}")

lead = dict(conn.execute("SELECT * FROM leads WHERE website_url IS NOT NULL LIMIT 1").fetchone())
conn.close()

print(f"\nTest sur : {lead['company_name']} (id={lead['id']})")

result = process_lead(lead)
status = result.pop("_status")
error  = result.pop("_error", None)
print(f"Status : {status} / Erreur : {error}")

if status == "extracted":
    q = LeadQueue("leads.db")
    fields = {k: v for k, v in result.items() if v is not None}
    print(f"\nChamps à écrire : {list(fields.keys())}")
    try:
        q.update_fields(lead["id"], **fields)
        print("update_fields : OK")
    except Exception as e:
        print(f"update_fields ERREUR : {e}")