import sys, re
from pathlib import Path
from difflib import SequenceMatcher

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

from config import config
from core.models import Lead, LeadStatus
from core.queue import LeadQueue
from services.serpapi import search_google_maps, parse_maps_result
from services.recherche_entreprises import search_entreprises
from services.gmb import enrich_from_maps_result
from services.dirigeant import find_dirigeant
from core.crm_filter import load_crm, filter_against_crm
from services.geocoding import find_expansion_cities


def _should_skip(d: dict):
    rating  = d.get("google_rating") or 0
    reviews = d.get("review_count") or 0
    if config.min_google_rating and rating < config.min_google_rating:
        return True, f"note trop basse ({rating})"
    if config.min_review_count and reviews < config.min_review_count:
        return True, f"trop peu d'avis ({reviews})"
    return False, ""


def _normalize(name) -> str:
    if not name:
        return ""
    name = str(name).lower().strip()
    for s in [" sarl", " sas", " eurl", " sa", " sci", " sasu"]:
        name = name.replace(s, "")
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def deduplicate_against_db(raw: list, existing: list, threshold: float = 0.85) -> tuple:
    """Supprime de raw les leads déjà présents dans les sessions précédentes (fuzzy match)."""
    filtered = []
    removed  = 0
    for lead in raw:
        n = _normalize(lead.get("company_name", ""))
        c = (lead.get("city") or "").lower()
        dup = False
        for ex in existing:
            if (ex.get("city") or "").lower() == c:
                ratio = SequenceMatcher(None, n, _normalize(ex.get("company_name", ""))).ratio()
                if ratio >= threshold:
                    dup = True
                    break
        if not dup:
            filtered.append(lead)
        else:
            removed += 1
    return filtered, removed


def deduplicate(leads: list, threshold: float = 0.85) -> list:
    seen = []
    for lead in leads:
        n = _normalize(lead.get("company_name", ""))
        c = (lead.get("city") or "").lower()
        dup = False
        for s in seen:
            if (s.get("city") or "").lower() == c:
                ratio = SequenceMatcher(None, n, _normalize(s.get("company_name", ""))).ratio()
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
        company_name    = str(d.get("company_name") or ""),
        city            = str(d.get("city") or ""),
        sector          = str(sector or ""),
        source          = str(d.get("source") or ""),
        address         = d.get("address"),
        google_rating   = d.get("google_rating"),
        review_count    = d.get("review_count"),
        website_url     = d.get("website_url"),
        phone           = d.get("phone") or None,
        gmb_confirmed   = bool(d.get("gmb_confirmed", False)),
        gmb_url         = d.get("gmb_url"),
        gmb_category    = d.get("gmb_category"),
        owner_name      = d.get("owner_name"),
        owner_role      = d.get("owner_role"),
        all_owners      = d.get("all_owners"),
        siren           = d.get("siren"),
        siret           = d.get("siret"),
        tva_intra       = d.get("tva_intra"),
        legal_form      = d.get("legal_form"),
        legal_form_code = d.get("legal_form_code"),
        etat            = d.get("etat"),
        capital_social  = d.get("capital_social"),
        naf_code        = d.get("naf_code"),
        naf_label       = d.get("naf_label"),
        section_activite= d.get("section_activite"),
        employee_range  = d.get("employee_range"),
        employee_code   = d.get("employee_code"),
        annee_effectif  = d.get("annee_effectif"),
        creation_date   = d.get("creation_date"),
        date_mise_a_jour= d.get("date_mise_a_jour"),
        siege_adresse   = d.get("siege_adresse"),
        siege_cp        = d.get("siege_cp"),
        siege_ville     = d.get("siege_ville"),
        nb_etablissements = d.get("nb_etablissements"),
        nb_etab_ouverts = d.get("nb_etab_ouverts"),
        est_ei          = d.get("est_ei"),
        est_association = d.get("est_association"),
        status          = LeadStatus.SCRAPED,
    )


class ScraperAgent:
    def __init__(self, queue: LeadQueue):
        self.queue = queue

    def _scrape_city(self, sector: str, city: str, use_maps: bool, use_registre: bool, max_results: int) -> list:
        """Scrape un secteur dans une ville donnée et retourne les leads bruts."""
        results = []
        if use_maps:
            count = 0
            try:
                for r in search_google_maps(sector, city, max_results=max_results):
                    p = parse_maps_result(r)
                    if not p or not p.get("company_name"):
                        continue
                    p["sector"] = sector
                    skip, _ = _should_skip(p)
                    if skip:
                        continue
                    try:
                        gmb = enrich_from_maps_result(r)
                        p.update(gmb)
                    except Exception:
                        log.warning("Enrichissement GMB echoue pour '%s'", p.get("company_name", "?"), exc_info=True)
                    results.append(p)
                    count += 1
            except Exception as e:
                log.error("Erreur Maps : %s", e)
            log.info("Maps : %d leads", count)

        if use_registre:
            count = 0
            try:
                entries = search_entreprises(sector, city, max_results=max_results or config.max_results_per_query)
                for e in entries:
                    if not e or not e.get("company_name"):
                        continue
                    e["sector"] = sector
                    results.append(e)
                    count += 1
            except Exception as e:
                log.error("Erreur Registre National : %s", e)
            log.info("Registre National : %d leads", count)

        return results

    async def run(self, queries: list, use_maps=True, use_registre=True, max_per_query=None) -> list:
        raw = []
        self.expanded_cities = {}  # {ville_demandée: [villes_explorées]}

        existing_leads = self.queue.get_existing_leads(exclude_session_id=self.queue.session_id)
        crm = load_crm()

        for sector, city in queries:
            sector = str(sector or "").strip()
            city   = str(city   or "").strip()
            if not sector or not city:
                continue

            target = max_per_query or config.max_results_per_query
            collected_for_query = []
            cities_explored = [city]

            # --- Scrape ville principale ---
            log.info("=== %s @ %s ===", sector, city)
            batch = self._scrape_city(sector, city, use_maps, use_registre, target)
            collected_for_query.extend(batch)

            # Dédup interne + cross-session pour évaluer le déficit
            deduped = deduplicate(list(collected_for_query))
            if existing_leads:
                deduped, _ = deduplicate_against_db(deduped, existing_leads)
            if crm:
                deduped, _ = filter_against_crm(deduped, crm)

            deficit = target - len(deduped)

            # --- Extension aux villes voisines si déficit ---
            if deficit > 0:
                log.info("Déficit de %d leads pour '%s @ %s' — recherche de villes voisines...", deficit, sector, city)
                nearby = find_expansion_cities(city, max_cities=15, max_radius_km=50)
                if nearby:
                    log.info("Villes voisines trouvées : %s", ", ".join(f"{c['nom']} ({c['distance_km']}km)" for c in nearby[:5]))

                for nc in nearby:
                    if deficit <= 0:
                        break
                    nc_name = nc["nom"]
                    log.info("--- Extension : %s @ %s (%s km) ---", sector, nc_name, nc["distance_km"])
                    cities_explored.append(f"{nc_name} ({nc['distance_km']}km)")

                    batch = self._scrape_city(sector, nc_name, use_maps, use_registre, min(deficit + 5, target))
                    collected_for_query.extend(batch)

                    # Recalculer le déficit après chaque ville
                    deduped = deduplicate(list(collected_for_query))
                    if existing_leads:
                        deduped, _ = deduplicate_against_db(deduped, existing_leads)
                    if crm:
                        deduped, _ = filter_against_crm(deduped, crm)
                    deficit = target - len(deduped)

                if len(cities_explored) > 1:
                    self.expanded_cities[f"{sector} @ {city}"] = cities_explored
                    log.info("Extension terminée : %d villes explorées pour '%s @ %s'", len(cities_explored), sector, city)

            raw.extend(collected_for_query)

        # Dédoublonnage interne (au sein du scrape actuel)
        before = len(raw)
        raw = deduplicate(raw)
        log.info("Dedoublonnage interne : %d -> %d", before, len(raw))

        # Dédoublonnage cross-sessions (contre la base existante)
        if existing_leads:
            before_db = len(raw)
            raw, nb_db_dups = deduplicate_against_db(raw, existing_leads)
            log.info("Dedoublonnage base : %d -> %d (%d déjà connus supprimés)", before_db, len(raw), nb_db_dups)
        else:
            nb_db_dups = 0
            log.info("Dedoublonnage base : base vide, aucun doublon à exclure")

        # Filtre CRM — exclut les entreprises déjà contactées
        if crm:
            raw, nb_exclus = filter_against_crm(raw, crm)
            log.info("Filtre CRM : %d doublons exclus — %d leads restants", nb_exclus, len(raw))
        else:
            log.info("Filtre CRM : aucun fichier CRM chargé")

        # Enrichissement dirigeant
        log.info("Recherche des dirigeants (%d entreprises)...", len(raw))
        found = 0
        for i, d in enumerate(raw):
            name = d.get("company_name") or ""
            city = d.get("city") or ""
            if not name:
                continue
            try:
                log.info("  [%d/%d] %s", i+1, len(raw), name[:40])
                info = find_dirigeant(name, city)
                if info.get("found"):
                    found += 1
                    for k, v in info.items():
                        if k != "found" and v is not None and not d.get(k):
                            d[k] = v
            except Exception as e:
                log.warning("Erreur dirigeant '%s' : %s", name[:30], e)

        log.info("Dirigeants trouvés : %d/%d", found, len(raw))

        # Sauvegarde
        leads = []
        for d in raw:
            if not d.get("company_name"):
                continue
            try:
                lead = _to_lead(d, d.get("sector", ""))
                self.queue.save(lead)
                leads.append(lead)
            except Exception as e:
                log.warning("Erreur sauvegarde '%s' : %s", d.get("company_name","?")[:30], e)

        log.info("Agent 1 terminé : %d leads | %d GMB | %d dirigeants",
            len(leads),
            sum(1 for l in leads if l.gmb_confirmed),
            sum(1 for l in leads if l.owner_name),
        )
        return leads
