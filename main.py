import sys
import asyncio
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from core.logger import get_logger, setup_logging
setup_logging()
log = get_logger(__name__)

from config import config
from core.queue import LeadQueue
from agents.scraper import ScraperAgent


def parse_args():
    p = argparse.ArgumentParser(description="Leads Engine — Agent 1 Scraping")
    p.add_argument("--sector",   required=True, help="Ex: plombier")
    p.add_argument("--city",     required=True, help="Ex: Marseille ou Lyon,Grenoble")
    p.add_argument("--max",      type=int, default=60, help="Resultats max par requete")
    p.add_argument("--no-maps",  action="store_true", help="Desactive Google Maps")
    p.add_argument("--no-registre", action="store_true", help="Desactive le Registre National")
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
        use_registre  = not args.no_registre,
        max_per_query = args.max,
    )

    stats = queue.stats()
    sep = "-" * 50
    print(f"\n{sep}")
    print(f"  RESULTATS AGENT 1")
    print(sep)
    print(f"  Nouveaux leads   : {len(leads)}")
    print(f"  (doublons base exclus automatiquement)")
    for status, count in stats.items():
        print(f"  {status:<14} : {count}")
    print(sep)
    print(f"  Base de donnees  : {config.db_path}")
    print(f"{sep}\n")


if __name__ == "__main__":
    asyncio.run(main())
