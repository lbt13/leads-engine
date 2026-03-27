"""
agents/extractor.py — Agent 2
Extraction technique via httpx (sans Playwright).
Plus rapide, plus fiable, fonctionne dans tous les contextes.
"""

import sys, re, time, asyncio, threading
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import httpx
from bs4 import BeautifulSoup
from core.models import LeadStatus
from core.queue import LeadQueue
from services.dns_lookup import get_hosting
from services.pagespeed import get_pagespeed

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


# ── Détection CMS ─────────────────────────────────────────────────────────────
CMS_SIGNATURES = [
    ("WordPress",    "url",  "/wp-content/"),
    ("WordPress",    "url",  "/wp-includes/"),
    ("Joomla",       "url",  "/components/com_"),
    ("Joomla",       "url",  "/media/jui/"),
    ("Drupal",       "url",  "/sites/default/files/"),
    ("Drupal",       "meta", "Drupal"),
    ("SPIP",         "url",  "/spip.php"),
    ("Prestashop",   "url",  "/modules/"),
    ("Prestashop",   "meta", "PrestaShop"),
    ("Magento",      "url",  "/skin/frontend/"),
    ("Shopify",      "url",  "cdn.shopify.com"),
    ("Shopify",      "meta", "Shopify"),
    ("Wix",          "url",  "static.wixstatic.com"),
    ("Squarespace",  "url",  "squarespace.com"),
    ("Squarespace",  "meta", "Squarespace"),
    ("Webflow",      "url",  "webflow.com"),
    ("Webflow",      "meta", "Webflow"),
    ("Jimdo",        "url",  "jimdo.com"),
    ("Weebly",       "url",  "weebly.com"),
    ("Webador",      "url",  "webador.fr"),
    ("TYPO3",        "meta", "TYPO3"),
    ("OVH Création", "url",  "ovhcloud.com"),
]

def detect_cms(html: str, headers: dict) -> str:
    if not html:
        return "Inconnu"
    html_l = html.lower()
    powered = (headers.get("x-powered-by") or "").lower()
    if "wordpress" in powered: return "WordPress"
    if "drupal"    in powered: return "Drupal"
    try:
        soup = BeautifulSoup(html, "lxml")
        gen  = soup.find("meta", attrs={"name": "generator"})
        if gen and gen.get("content"):
            c = gen["content"].lower()
            for cms, t, sig in CMS_SIGNATURES:
                if t == "meta" and sig.lower() in c:
                    return cms
    except Exception:
        log.warning("detect_cms parsing echoue", exc_info=True)
    for cms, t, sig in CMS_SIGNATURES:
        if t == "url" and sig.lower() in html_l:
            return cms
    if re.search(r'wp-content/themes/[^"]+\?ver=', html):
        return "WordPress"
    return "Inconnu"


# ── Détection agence ──────────────────────────────────────────────────────────
AGENCE_PATTERNS = [
    r"(?:réalisé|créé|conçu|développé)\s+par\s+([\w\s\-&À-ÿ]{3,50})(?:\.|,|<)",
    r"design\s+(?:by|par)\s+([\w\s\-&À-ÿ]{3,50})(?:\.|,|<|\")",
    r"powered\s+by\s+([\w\s\-&À-ÿ]{3,50})(?:\.|,|<)",
]

def detect_agence(html: str) -> str:
    for pattern in AGENCE_PATTERNS:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            a = m.group(1).strip()
            if 3 < len(a) < 60:
                return a.title()
    return ""


# ── SEO keywords ──────────────────────────────────────────────────────────────
def detect_seo(html: str, soup) -> dict:
    signals = {}
    try:
        desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        signals["meta_description"] = bool(desc and desc.get("content","").strip())

        kw = soup.find("meta", attrs={"name": re.compile(r"keywords", re.I)})
        signals["meta_keywords"] = bool(kw and kw.get("content","").strip())

        signals["og_tags"]    = bool(soup.find("meta", attrs={"property": re.compile(r"^og:")}))
        signals["schema_org"] = bool(soup.find_all("script", attrs={"type":"application/ld+json"}))
        signals["canonical"]  = bool(soup.find("link", attrs={"rel": re.compile(r"canonical",re.I)}))
        signals["heading_h1"] = bool(soup.find("h1"))
        signals["heading_h2"] = bool(soup.find("h2"))

        imgs = soup.find_all("img")
        signals["img_alt"]    = bool(any(img.get("alt","").strip() for img in imgs))
        signals["analytics"]  = bool("google-analytics.com" in html or "gtag(" in html or "googletagmanager.com" in html)

        title = soup.find("title")
        signals["title_tag"]  = bool(title and title.get_text(strip=True))

    except Exception as e:
        log.debug("detect_seo erreur : %s", e)
        return {"has_seo_keywords": False, "seo_signals": "erreur", "seo_score": 0}

    # Labels lisibles pour les signaux manquants (ceux déjà couverts par structure sont exclus)
    MISSING_LABELS = {
        "title_tag":    "Balise title absente",
        "og_tags":      "Balises Open Graph absentes",
        "schema_org":   "Pas de données structurées (schema.org)",
        "canonical":    "Balise canonical absente",
        "heading_h2":   "Aucun H2",
        "img_alt":      "Images sans attribut alt",
        # meta_description, heading_h1, analytics → déjà dans compute_weaknesses via structure
    }

    present = [k for k,v in signals.items() if v]
    missing_labels = [
        MISSING_LABELS[k] for k, v in signals.items()
        if not v and k in MISSING_LABELS
    ]

    has_seo = (signals.get("title_tag") and
               signals.get("meta_description") and
               signals.get("heading_h1"))
    return {
        "has_seo_keywords": bool(has_seo),
        "seo_signals":      ", ".join(present) if present else "aucun",
        "seo_score":        len(present),
        "seo_missing":      missing_labels,
    }


# ── Contacts ──────────────────────────────────────────────────────────────────
def extract_email(html: str) -> str:
    raw = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", html)
    blacklist = {"noreply","no-reply","exemple","example","test@","email@","user@"}
    seen, result = set(), []
    for e in raw:
        el = e.lower()
        if el not in seen and not any(b in el for b in blacklist):
            result.append(e); seen.add(el)
    if not result: return ""
    priority = ["contact","info","bonjour","hello","accueil","pro"]
    result.sort(key=lambda e: 0 if any(k in e.lower() for k in priority) else 1)
    return result[0]

def extract_phone(html: str) -> str:
    raw = re.findall(r"(?:(?:\+33|0033|0)\s*[\.\-\s]?)(?:[1-9]\s*(?:[\.\-\s]?\d{2}){4})", html)
    for p in raw:
        c = re.sub(r"\D","",p)
        if len(c) in (10,11): return c
    return ""


# ── Structure ─────────────────────────────────────────────────────────────────
def analyze_structure(soup, html: str) -> dict:
    try:
        title   = soup.find("title")
        desc    = soup.find("meta", attrs={"name": re.compile(r"description",re.I)})
        h1s     = soup.find_all("h1")
        vp      = soup.find("meta", attrs={"name":"viewport"})
        imgs    = soup.find_all("img")
        c_years = re.findall(r"©\s*(\d{4})", html)
        return {
            "page_title":       (title.get_text(strip=True) if title else ""),
            "has_meta_desc":    bool(desc and desc.get("content","").strip()),
            "h1_count":         len(h1s),
            "is_responsive":    bool(vp),
            "has_analytics":    bool("google-analytics" in html or "gtag(" in html or "googletagmanager" in html),
            "copyright_year":   min(c_years) if c_years else "",
            "imgs_without_alt": sum(1 for img in imgs if not img.get("alt","").strip()),
        }
    except Exception as e:
        log.debug("analyze_structure erreur : %s", e)
        return {"page_title":"","has_meta_desc":False,"h1_count":0,
                "is_responsive":False,"has_analytics":False,"copyright_year":"","imgs_without_alt":0}


# ── Faiblesses ────────────────────────────────────────────────────────────────
def compute_weaknesses(structure: dict, ps_mobile, cms: str, seo: dict) -> str:
    w = []
    if ps_mobile is not None:
        if   ps_mobile < 30: w.append(f"Site très lent ({ps_mobile}/100)")
        elif ps_mobile < 50: w.append(f"Performance faible ({ps_mobile}/100)")
        elif ps_mobile < 70: w.append(f"Performance moyenne ({ps_mobile}/100)")
    if not seo.get("has_seo_keywords"):       w.append("SEO inexistant")
    elif (seo.get("seo_score") or 0) < 4:    w.append(f"SEO limité ({seo.get('seo_score',0)}/10)")
    if not structure.get("has_meta_desc"):    w.append("Pas de meta description")
    if structure.get("h1_count",0) == 0:     w.append("Aucun H1")
    elif structure.get("h1_count",0) > 1:    w.append(f"{structure['h1_count']} balises H1")
    if not structure.get("is_responsive"):    w.append("Non responsive")
    if not structure.get("has_analytics"):    w.append("Pas d'analytics")
    if cms in ("Wix","Webador","Jimdo","Weebly"): w.append(f"CMS gratuit ({cms})")
    y = structure.get("copyright_year","")
    if y and str(y).isdigit() and int(y) < 2019: w.append(f"Site ancien (© {y})")
    # Signaux SEO manquants qui ont fait baisser le score
    for label in (seo.get("seo_missing") or []):
        w.append(label)
    return "|".join(w)


# ── Visite Playwright (fallback JS) ──────────────────────────────────────────
# ── Visite Playwright (fallback JS) ──────────────────────────────────────────
async def _fetch_html_playwright_async(url: str) -> dict:
    """Fallback Playwright pour les sites qui bloquent httpx ou nécessitent JS."""
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    result = {"html": "", "headers": {}, "final_url": url, "is_https": False}
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                locale="fr-FR",
                viewport={"width": 1280, "height": 800},
                user_agent=HEADERS["User-Agent"],
            )
            page = await ctx.new_page()
            await page.route(
                "**/*.{png,jpg,jpeg,gif,webp,woff,woff2,svg}",
                lambda r: r.abort()
            )
            resp = None
            try:
                resp = await page.goto(url, timeout=20000, wait_until="domcontentloaded")
            except PWTimeout:
                log.warning("Playwright timeout : %s", url)
            except Exception as e:
                log.debug("Playwright goto erreur : %s", e)

            if resp:
                html = await page.content()
                result["html"]      = html
                result["headers"]   = await resp.all_headers()
                result["final_url"] = page.url
                result["is_https"]  = page.url.startswith("https://")
            await browser.close()
    except Exception as e:
        log.debug("Playwright erreur globale %s : %s", url, e)
    return result


def _fetch_html_playwright_sync(url: str) -> dict:
    """Wrapper synchrone pour _fetch_html_playwright_async."""
    result_box, error_box = [], []

    def run():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result_box.append(loop.run_until_complete(_fetch_html_playwright_async(url)))
            loop.close()
        except Exception as e:
            error_box.append(e)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=40)
    if error_box:
        log.warning("Playwright thread erreur : %s", error_box[0])
    return result_box[0] if result_box else {"html": "", "headers": {}, "final_url": url, "is_https": False}


# ── Visite httpx (remplace Playwright) ───────────────────────────────────────
def _fetch_html(url: str) -> dict:
    """
    Récupère le HTML via httpx — plus simple, plus fiable que Playwright
    pour la détection de CMS, emails et SEO (pas besoin de JS).
    """
    result = {"html": "", "headers": {}, "final_url": url, "is_https": False}

    # Essaie d'abord https, puis http si nécessaire
    urls_to_try = []
    if url.startswith("http"):
        urls_to_try.append(url)
        if url.startswith("https://"):
            urls_to_try.append(url.replace("https://", "http://", 1))
        else:
            urls_to_try.append(url.replace("http://", "https://", 1))
    else:
        urls_to_try = [f"https://{url}", f"http://{url}"]

    for try_url in urls_to_try:
        try:
            with httpx.Client(
                headers=HEADERS,
                timeout=15,
                follow_redirects=True,
                verify=False,        # ignore les certs expirés
            ) as client:
                r = client.get(try_url)
                if r.status_code < 400:
                    result["html"]      = r.text
                    result["headers"]   = dict(r.headers)
                    result["final_url"] = str(r.url)
                    result["is_https"]  = str(r.url).startswith("https://")
                    return result
        except httpx.TimeoutException:
            log.warning("Timeout httpx : %s", try_url)
        except Exception as e:
            log.debug("httpx erreur %s : %s", try_url, e)

    return result


# ── Traitement d'un lead ──────────────────────────────────────────────────────
def process_lead(lead_dict: dict) -> dict:
    url = lead_dict.get("website_url") or ""
    if not url:
        return {"_status": "skipped", "_error": "pas de site web"}
    if not url.startswith("http"):
        url = "https://" + url

    log.info("    → %s", url)

    try:
        # 1. Fetch HTML via httpx, fallback Playwright si résultat insuffisant
        visit   = _fetch_html(url)
        html    = visit.get("html") or ""
        headers = visit.get("headers") or {}

        if not html or len(html) < 200:
            log.info("    → httpx insuffisant, retry Playwright : %s", url)
            visit   = _fetch_html_playwright_sync(url)
            html    = visit.get("html") or ""
            headers = visit.get("headers") or {}

        if not html or len(html) < 200:
            return {"_status": "error", "_error": f"site inaccessible ({url})"}

        # 2. Parse
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            return {"_status": "error", "_error": f"parsing échoué : {e}"}

        # 3. Analyses
        cms       = detect_cms(html, headers)
        domain    = urlparse(visit.get("final_url") or url).netloc.replace("www.","")
        hosting   = get_hosting(domain) or "Inconnu"
        agence    = detect_agence(html)
        email     = extract_email(html)
        phone     = extract_phone(html)
        structure = analyze_structure(soup, html)
        seo       = detect_seo(html, soup)

        # 4. PageSpeed
        try:
            ps = get_pagespeed(url)
        except Exception:
            log.warning("PageSpeed echoue pour %s", url, exc_info=True)
            ps = {"mobile": None, "desktop": None}

        # 5. Faiblesses
        weaknesses = compute_weaknesses(structure, ps.get("mobile"), cms, seo)

        # 6. Résultat — tout converti en types SQLite-compatibles
        return {
            "_status":           "extracted",
            "_error":            None,
            "email":             email or None,
            "phone":             phone or lead_dict.get("phone") or None,
            "cms":               cms,
            "hosting":           hosting,
            "agence_web":        agence or None,
            "is_https":          1 if visit.get("is_https") else 0,
            "pagespeed_mobile":  ps.get("mobile"),
            "pagespeed_desktop": ps.get("desktop"),
            "page_title":        structure.get("page_title") or None,
            "has_meta_desc":     1 if structure.get("has_meta_desc") else 0,
            "h1_count":          int(structure.get("h1_count") or 0),
            "is_responsive":     1 if structure.get("is_responsive") else 0,
            "has_analytics":     1 if structure.get("has_analytics") else 0,
            "copyright_year":    structure.get("copyright_year") or None,
            "has_seo_keywords":  1 if seo.get("has_seo_keywords") else 0,
            "seo_signals":       seo.get("seo_signals") or "",
            "seo_score":         int(seo.get("seo_score") or 0),
            "seo_weaknesses":    weaknesses,
        }

    except Exception as e:
        log.error("process_lead exception %s : %s", url, e, exc_info=True)
        return {"_status": "error", "_error": str(e)}


# ── Agent principal ───────────────────────────────────────────────────────────
class ExtractorAgent:
    def __init__(self, queue: LeadQueue):
        self.queue = queue

    def run(self, leads: list, delay: float = 2.0) -> dict:
        total, success, skipped, errors = len(leads), 0, 0, 0
        log.info("Agent 2 démarré — %d leads", total)

        for i, lead in enumerate(leads):
            lead_id = lead.get("id")
            name    = (lead.get("company_name") or "")[:40]
            log.info("[%d/%d] %s", i+1, total, name)

            if not lead_id:
                log.warning("Lead sans ID ignoré")
                errors += 1
                continue

            result  = process_lead(lead)
            status  = result.pop("_status", "error")
            error   = result.pop("_error", None)

            if status == "skipped":
                self.queue.update_status(lead_id, LeadStatus.SKIPPED, error)
                skipped += 1
            elif status == "error":
                log.warning("  Erreur : %s", error)
                self.queue.update_status(lead_id, LeadStatus.ERROR, error)
                errors += 1
            else:
                fields = {k: v for k, v in result.items() if v is not None}
                try:
                    if fields:
                        self.queue.update_fields(lead_id, **fields)
                    self.queue.update_status(lead_id, LeadStatus.EXTRACTED)
                    success += 1
                    log.info("  OK — CMS:%s Hébergeur:%s SEO:%s Speed:%s",
                        result.get("cms","?"),
                        result.get("hosting","?"),
                        result.get("has_seo_keywords","?"),
                        result.get("pagespeed_mobile","—"),
                    )
                except Exception as e:
                    log.error("  Sauvegarde erreur : %s", e)
                    self.queue.update_status(lead_id, LeadStatus.ERROR, str(e))
                    errors += 1

            time.sleep(delay)

        log.info("Agent 2 terminé : %d ok | %d skipped | %d erreurs", success, skipped, errors)
        return {"success": success, "skipped": skipped, "errors": errors, "total": total}
