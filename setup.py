"""
setup.py — Lance ce fichier UNE seule fois depuis n'importe quel dossier.
Il installe les dependances ET cree tous les fichiers du projet.

Usage :
    1. Place ce fichier dans un dossier vide (ex: Bureau/leads_engine)
    2. Ouvre un CMD dans ce dossier
    3. python setup.py
"""

import os
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent

def ok(msg):    print(f"  [OK]  {msg}")
def err(msg):   print(f"\n  [ERREUR] {msg}\n"); sys.exit(1)
def info(msg):  print(f"  ...   {msg}")
def titre(msg): print(f"\n{'='*55}\n  {msg}\n{'='*55}")


# ═══════════════════════════════════════════════════════════
titre("Etape 1/5 — Verification Python")
# ═══════════════════════════════════════════════════════════
v = sys.version_info
if v.major < 3 or v.minor < 10:
    err(f"Python 3.10+ requis. Tu as Python {v.major}.{v.minor}.\nTelechargez-le sur https://python.org")
ok(f"Python {v.major}.{v.minor}.{v.micro}")


# ═══════════════════════════════════════════════════════════
titre("Etape 2/5 — Creation de la structure du projet")
# ═══════════════════════════════════════════════════════════

DOSSIERS = ["core", "agents", "services", "output"]
for d in DOSSIERS:
    (ROOT / d).mkdir(exist_ok=True)
    (ROOT / d / "__init__.py").touch()
    ok(f"Dossier {d}/")


# ═══════════════════════════════════════════════════════════
titre("Etape 3/5 — Creation des fichiers Python")
# ═══════════════════════════════════════════════════════════

FICHIERS = {}

# ───────────────────────────────────────────────────────────
FICHIERS["core/models.py"] = """\
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime


class LeadStatus(Enum):
    SCRAPED   = "scraped"
    EXTRACTED = "extracted"
    ANALYZED  = "analyzed"
    SCORED    = "scored"
    EXPORTED  = "exported"
    SKIPPED   = "skipped"
    ERROR     = "error"


@dataclass
class Lead:
    company_name:      str
    city:              str
    sector:            str
    source:            str
    address:           Optional[str]   = None
    google_rating:     Optional[float] = None
    review_count:      Optional[int]   = None
    website_url:       Optional[str]   = None
    phone:             Optional[str]   = None
    email:             Optional[str]   = None
    contact_name:      Optional[str]   = None
    cms:               Optional[str]   = None
    hosting:           Optional[str]   = None
    pagespeed_mobile:  Optional[int]   = None
    pagespeed_desktop: Optional[int]   = None
    page_count:        Optional[int]   = None
    seo_weaknesses:    list            = field(default_factory=list)
    seo_summary:       Optional[str]   = None
    hook:              Optional[str]   = None
    score:             Optional[float] = None
    status:            LeadStatus      = LeadStatus.SCRAPED
    error_message:     Optional[str]   = None
    scraped_at:        str             = field(
        default_factory=lambda: datetime.now().isoformat()
    )

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        d["status"] = self.status.value
        d["seo_weaknesses"] = "|".join(self.seo_weaknesses)
        return d
"""

# ───────────────────────────────────────────────────────────
FICHIERS["core/queue.py"] = """\
import sqlite3
import logging
from datetime import datetime
from core.models import Lead, LeadStatus

log = logging.getLogger(__name__)

DDL = '''
CREATE TABLE IF NOT EXISTS leads (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name      TEXT NOT NULL,
    city              TEXT,
    sector            TEXT,
    source            TEXT,
    address           TEXT,
    google_rating     REAL,
    review_count      INTEGER,
    website_url       TEXT,
    phone             TEXT,
    email             TEXT,
    contact_name      TEXT,
    cms               TEXT,
    hosting           TEXT,
    pagespeed_mobile  INTEGER,
    pagespeed_desktop INTEGER,
    page_count        INTEGER,
    seo_weaknesses    TEXT,
    seo_summary       TEXT,
    hook              TEXT,
    score             REAL,
    status            TEXT DEFAULT "scraped",
    error_message     TEXT,
    scraped_at        TEXT,
    updated_at        TEXT,
    UNIQUE(company_name, city, source)
);
CREATE INDEX IF NOT EXISTS idx_status ON leads(status);
'''


class LeadQueue:
    def __init__(self, db_path: str = "leads.db"):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(DDL)

    def save(self, lead: Lead) -> int:
        d = lead.to_dict()
        d["updated_at"] = datetime.now().isoformat()
        cols         = ", ".join(d.keys())
        placeholders = ", ".join(["?"] * len(d))
        updates      = ", ".join(
            f"{k}=excluded.{k}" for k in d
            if k not in ("company_name", "city", "source")
        )
        sql = (
            f"INSERT INTO leads ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT(company_name, city, source) DO UPDATE SET {updates}"
        )
        with self._conn() as conn:
            cur = conn.execute(sql, list(d.values()))
            return cur.lastrowid

    def update_status(self, lead_id: int, status: LeadStatus, error: str = None):
        with self._conn() as conn:
            conn.execute(
                "UPDATE leads SET status=?, error_message=?, updated_at=? WHERE id=?",
                (status.value, error, datetime.now().isoformat(), lead_id),
            )

    def update_fields(self, lead_id: int, **kwargs):
        kwargs["updated_at"] = datetime.now().isoformat()
        sets = ", ".join(f"{k}=?" for k in kwargs)
        with self._conn() as conn:
            conn.execute(
                f"UPDATE leads SET {sets} WHERE id=?",
                [*kwargs.values(), lead_id]
            )

    def get_by_status(self, status: LeadStatus) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM leads WHERE status=? ORDER BY id",
                (status.value,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_all_scored(self, min_score: float = 0.0) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM leads WHERE status='scored' AND score>=? ORDER BY score DESC",
                (min_score,)
            ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as n FROM leads GROUP BY status"
            ).fetchall()
        return {r["status"]: r["n"] for r in rows}
"""

# ───────────────────────────────────────────────────────────
FICHIERS["config.py"] = """\
import os
from dataclasses import dataclass, field


@dataclass
class Config:
    serpapi_key:           str   = ""
    anthropic_key:         str   = ""
    pagespeed_key:         str   = ""
    max_results_per_query: int   = 60
    playwright_timeout_ms: int   = 15000
    playwright_headless:   bool  = True
    delay_between_pages_s: float = 2.0
    delay_serpapi_s:       float = 1.0
    min_google_rating:     float = 0.0
    min_review_count:      int   = 0
    skip_cms:              list  = field(default_factory=list)
    output_dir:            str   = "output"
    db_path:               str   = "leads.db"

    def __post_init__(self):
        self.serpapi_key   = os.getenv("SERPAPI_KEY", "")
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.pagespeed_key = os.getenv("PAGESPEED_API_KEY", "")


config = Config()
"""

# ───────────────────────────────────────────────────────────
FICHIERS["services/serpapi.py"] = """\
import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from config import config

log = logging.getLogger(__name__)
ENDPOINT = "https://serpapi.com/search.json"


def search_google_maps(query: str, location: str, max_results: int = None):
    max_results = max_results or config.max_results_per_query
    start, fetched = 0, 0
    while fetched < max_results:
        params = {
            "engine":  "google_maps",
            "q":       f"{query} {location}",
            "type":    "search",
            "hl":      "fr",
            "gl":      "fr",
            "start":   start,
            "api_key": config.serpapi_key,
        }
        try:
            r = httpx.get(ENDPOINT, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("SerpAPI erreur : %s", e)
            break
        results = data.get("local_results", [])
        if not results:
            break
        for item in results:
            if fetched >= max_results:
                return
            yield item
            fetched += 1
        if len(results) < 20:
            break
        start += 20
        time.sleep(config.delay_serpapi_s)


def parse_maps_result(raw: dict):
    name = raw.get("title", "").strip()
    if not name:
        return None
    website = (
        raw.get("website")
        or raw.get("links", {}).get("website")
        or ""
    ).strip().rstrip("/")
    address = raw.get("address", "")
    city = address.split(",")[-1].strip() if address else ""
    return {
        "company_name":  name,
        "address":       address,
        "city":          city,
        "phone":         raw.get("phone", ""),
        "google_rating": raw.get("rating"),
        "review_count":  raw.get("reviews"),
        "website_url":   website or None,
        "source":        "google_maps",
    }
"""

# ───────────────────────────────────────────────────────────
FICHIERS["services/pages_jaunes.py"] = """\
import sys
import asyncio
import logging
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from config import config

log = logging.getLogger(__name__)
BASE = "https://www.pagesjaunes.fr/annuaire/chercherlespros"

SEL = {
    "cards":        "article.bi-generic",
    "name":         "a.denomination-links span[itemprop='name']",
    "address":      "span.adresse-complete",
    "phone":        "a[data-pj-event='click:profil-secondaire:appeler']",
    "website":      "a[data-pj-event*='site-internet']",
    "rating":       "span.note-avis",
    "review_count": "span.nb-avis",
    "next":         "a.pagination-next",
}


async def scrape_pages_jaunes(query: str, location: str, max_results: int = None):
    max_results = max_results or config.max_results_per_query
    fetched, page_num = 0, 1

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=config.playwright_headless)
        ctx = await browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,woff,woff2}",
            lambda r: r.abort()
        )

        while fetched < max_results:
            url = (
                f"{BASE}?quoiqui={query.replace(' ', '+')}"
                f"&ou={location.replace(' ', '+')}&page={page_num}"
            )
            log.info("Pages Jaunes p%d : %s %s", page_num, query, location)
            try:
                await page.goto(
                    url,
                    timeout=config.playwright_timeout_ms,
                    wait_until="domcontentloaded"
                )
                await page.wait_for_selector(SEL["cards"], timeout=8000)
            except PWTimeout:
                log.warning("Timeout page %d", page_num)
                break
            except Exception as e:
                log.error("Erreur Playwright : %s", e)
                break

            cards = await page.query_selector_all(SEL["cards"])
            if not cards:
                break

            for card in cards:
                if fetched >= max_results:
                    break
                entry = await _parse_card(card)
                if entry:
                    yield entry
                    fetched += 1

            next_btn = await page.query_selector(SEL["next"])
            if not next_btn:
                break
            page_num += 1
            await asyncio.sleep(config.delay_between_pages_s)

        await browser.close()


async def _parse_card(card) -> dict:
    async def txt(sel):
        el = await card.query_selector(sel)
        return (await el.inner_text()).strip() if el else ""

    async def attr(sel, a):
        el = await card.query_selector(sel)
        return (await el.get_attribute(a) or "").strip() if el else ""

    name = await txt(SEL["name"])
    if not name:
        return None

    address  = await txt(SEL["address"])
    phone    = await txt(SEL["phone"])
    website  = await attr(SEL["website"], "href")
    rating_s = await txt(SEL["rating"])
    reviews_s = await txt(SEL["review_count"])

    rating = None
    if rating_s:
        m = re.search(r"([\\d,]+)", rating_s)
        if m:
            rating = float(m.group(1).replace(",", "."))

    review_count = None
    if reviews_s:
        m = re.search(r"(\\d+)", reviews_s)
        if m:
            review_count = int(m.group(1))

    city = ""
    if address:
        m = re.search(r"\\b\\d{5}\\s+(.+)$", address)
        city = m.group(1).strip() if m else address.split(",")[-1].strip()

    phone_clean = re.sub(r"\\D", "", phone)

    return {
        "company_name":  name,
        "address":       address,
        "city":          city,
        "phone":         phone_clean or None,
        "google_rating": rating,
        "review_count":  review_count,
        "website_url":   website or None,
        "source":        "pages_jaunes",
    }
"""

# ───────────────────────────────────────────────────────────
FICHIERS["agents/scraper.py"] = """\
import sys
import asyncio
import logging
import re
from pathlib import Path
from difflib import SequenceMatcher

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import config
from core.models import Lead, LeadStatus
from core.queue import LeadQueue
from services.serpapi import search_google_maps, parse_maps_result
from services.pages_jaunes import scrape_pages_jaunes

log = logging.getLogger(__name__)


def _should_skip(d: dict):
    rating  = d.get("google_rating") or 0
    reviews = d.get("review_count") or 0
    if config.min_google_rating and rating < config.min_google_rating:
        return True, f"note trop basse ({rating})"
    if config.min_review_count and reviews < config.min_review_count:
        return True, f"trop peu d'avis ({reviews})"
    return False, ""


def _normalize(name: str) -> str:
    name = name.lower().strip()
    for s in [" sarl", " sas", " eurl", " sa", " sci", " sasu"]:
        name = name.replace(s, "")
    name = re.sub(r"[^a-z0-9\\s]", " ", name)
    return re.sub(r"\\s+", " ", name).strip()


def deduplicate(leads: list, threshold: float = 0.85) -> list:
    seen = []
    for lead in leads:
        n = _normalize(lead["company_name"])
        c = lead.get("city", "").lower()
        dup = False
        for s in seen:
            if s.get("city", "").lower() == c:
                ratio = SequenceMatcher(
                    None, n, _normalize(s["company_name"])
                ).ratio()
                if ratio >= threshold:
                    if lead.get("website_url") and not s.get("website_url"):
                        seen.remove(s)
                        seen.append(lead)
                    dup = True
                    break
        if not dup:
            seen.append(lead)
    return seen


def _to_lead(d: dict, sector: str) -> Lead:
    return Lead(
        company_name  = d["company_name"],
        city          = d.get("city", ""),
        sector        = sector,
        source        = d.get("source", ""),
        address       = d.get("address"),
        google_rating = d.get("google_rating"),
        review_count  = d.get("review_count"),
        website_url   = d.get("website_url"),
        phone         = d.get("phone") or None,
        status        = LeadStatus.SCRAPED,
    )


class ScraperAgent:
    def __init__(self, queue: LeadQueue):
        self.queue = queue

    async def run(
        self,
        queries: list,
        use_maps: bool = True,
        use_pj:   bool = True,
        max_per_query: int = None,
    ) -> list:
        raw = []

        for sector, city in queries:
            log.info("=== %s @ %s ===", sector, city)

            if use_maps:
                count = 0
                for r in search_google_maps(sector, city, max_results=max_per_query):
                    p = parse_maps_result(r)
                    if not p:
                        continue
                    p["sector"] = sector
                    skip, _ = _should_skip(p)
                    if not skip:
                        raw.append(p)
                        count += 1
                log.info("Maps : %d leads", count)

            if use_pj:
                count = 0
                async for e in scrape_pages_jaunes(sector, city, max_results=max_per_query):
                    e["sector"] = sector
                    skip, _ = _should_skip(e)
                    if not skip:
                        raw.append(e)
                        count += 1
                log.info("Pages Jaunes : %d leads", count)

        before = len(raw)
        raw = deduplicate(raw)
        log.info("Dedoublonnage : %d -> %d", before, len(raw))

        leads = []
        for d in raw:
            lead = _to_lead(d, d.get("sector", ""))
            self.queue.save(lead)
            leads.append(lead)

        log.info("Agent 1 termine : %d leads sauvegardes", len(leads))
        return leads
"""

# ───────────────────────────────────────────────────────────
FICHIERS["main.py"] = """\
import sys
import asyncio
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from config import config
from core.queue import LeadQueue
from agents.scraper import ScraperAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Leads Engine — Agent 1 Scraping")
    p.add_argument("--sector",   required=True, help="Ex: plombier")
    p.add_argument("--city",     required=True, help="Ex: Marseille ou Lyon,Grenoble")
    p.add_argument("--max",      type=int, default=60, help="Resultats max par requete")
    p.add_argument("--no-maps",  action="store_true", help="Desactive Google Maps")
    p.add_argument("--no-pj",    action="store_true", help="Desactive Pages Jaunes")
    return p.parse_args()


async def main():
    args = parse_args()

    if not args.no_maps and not config.serpapi_key:
        log.error("SERPAPI_KEY manquante dans .env — utilise --no-maps pour tester sans")
        sys.exit(1)

    cities  = [c.strip() for c in args.city.split(",")]
    sectors = [s.strip() for s in args.sector.split(",")]
    queries = [(sec, city) for sec in sectors for city in cities]

    queue = LeadQueue(config.db_path)
    agent = ScraperAgent(queue)

    leads = await agent.run(
        queries       = queries,
        use_maps      = not args.no_maps,
        use_pj        = not args.no_pj,
        max_per_query = args.max,
    )

    stats = queue.stats()
    sep = "-" * 50
    print(f"\\n{sep}")
    print(f"  RESULTATS AGENT 1")
    print(sep)
    print(f"  Leads collectes  : {len(leads)}")
    for status, count in stats.items():
        print(f"  {status:<14} : {count}")
    print(sep)
    print(f"  Base de donnees  : {config.db_path}")
    print(f"{sep}\\n")


if __name__ == "__main__":
    asyncio.run(main())
"""

# ───────────────────────────────────────────────────────────
FICHIERS[".env"] = """\
SERPAPI_KEY=
ANTHROPIC_API_KEY=
PAGESPEED_API_KEY=
"""

FICHIERS["requirements.txt"] = """\
httpx>=0.27
playwright>=1.44
google-search-results>=2.4
dnspython>=2.6
beautifulsoup4>=4.12
lxml>=5.2
anthropic>=0.28
python-dotenv>=1.0
tqdm>=4.66
"""

# ─── Ecriture des fichiers ──────────────────────────────────
for chemin, contenu in FICHIERS.items():
    path = ROOT / chemin
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contenu, encoding="utf-8")
    ok(f"Cree : {chemin}")


# ═══════════════════════════════════════════════════════════
titre("Etape 4/5 — Installation des dependances pip")
# ═══════════════════════════════════════════════════════════

info("Installation en cours (1-3 minutes selon ta connexion)...")
result = subprocess.run(
    [sys.executable, "-m", "pip", "install", "-r", str(ROOT / "requirements.txt"), "--quiet"],
    capture_output=True, text=True
)
if result.returncode != 0:
    print(result.stderr[-1000:])
    err("Echec pip install. Verifie ta connexion internet.")
ok("Toutes les dependances Python installees")


# ═══════════════════════════════════════════════════════════
titre("Etape 5/5 — Installation Playwright (navigateur Chromium)")
# ═══════════════════════════════════════════════════════════

info("Telechargement Chromium (~100 Mo, une seule fois)...")
result = subprocess.run(
    [sys.executable, "-m", "playwright", "install", "chromium"],
    capture_output=True, text=True
)
if result.returncode != 0:
    print(result.stderr[-500:])
    err("Echec installation Playwright.")
ok("Chromium installe")


# ═══════════════════════════════════════════════════════════
print(f"""
{'='*55}
  INSTALLATION TERMINEE
{'='*55}

  AVANT DE LANCER, remplis tes cles API :
  Ouvre le fichier .env et ajoute :

    SERPAPI_KEY=ta_cle_serpapi
    ANTHROPIC_API_KEY=ta_cle_anthropic

  Obtenir les cles :
    SerpAPI   -> https://serpapi.com  (100 req gratuites)
    Anthropic -> https://console.anthropic.com

  POUR TESTER SANS CLES (Pages Jaunes seulement) :
    python main.py --sector "plombier" --city "Marseille" --max 20 --no-maps

  POUR LANCER EN COMPLET :
    python main.py --sector "plombier" --city "Marseille" --max 50

  PLUSIEURS VILLES D'UN COUP :
    python main.py --sector "electricien" --city "Lyon,Grenoble,Valence" --max 30

{'='*55}
""")
