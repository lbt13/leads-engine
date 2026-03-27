import sys, sqlite3, logging
from pathlib import Path

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parent))

from core.queue import LeadQueue
from agents.extractor import process_lead

# Récupère le premier lead avec un site web
conn = sqlite3.connect("leads.db")
conn.row_factory = sqlite3.Row
lead = conn.execute(
    "SELECT * FROM leads WHERE website_url IS NOT NULL LIMIT 1"
).fetchone()
conn.close()

if not lead:
    print("Aucun lead avec site web trouvé")
else:
    lead = dict(lead)
    print(f"\nTest sur : {lead['company_name']} — {lead['website_url']}\n")
    result = process_lead(lead)
    print("\n=== RÉSULTAT ===")
    for k, v in result.items():
        print(f"  {k:<25} : {v}")