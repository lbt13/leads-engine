"""
agents/extractor.py — Agent 2
Extraction technique via httpx.
Plus rapide, plus fiable, fonctionne dans tous les contextes.
"""

import sys, re, time, asyncio, threading
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime

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
    "User-Agent": "LeadsEngine/12.0 (+https://leadsengine.netlify.app; contact: leadsengine.contact@gmail.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


# ── Respect robots.txt ───────────────────────────────────────────────────────
_robots_cache: dict[str, bool] = {}


def _is_allowed_by_robots(url: str) -> bool:
    from urllib.robotparser import RobotFileParser
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base in _robots_cache:
        return _robots_cache[base]
    try:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        rp.read()
        allowed = rp.can_fetch(HEADERS["User-Agent"], url)
        _robots_cache[base] = allowed
        return allowed
    except Exception:
        _robots_cache[base] = True
        return True


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
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_EMAIL_BLACKLIST = {"noreply","no-reply","no_reply","exemple","example","test@","email@","user@",
                    "sentry","webpack","devtools","localhost","wixpress","sentry.io"}
_EMAIL_PRIORITY = ["contact","info","bonjour","hello","accueil","pro","commercial","direction"]

CONTACT_PATHS = [
    "/contact", "/contactez-nous", "/nous-contacter",
    "/a-propos", "/about", "/about-us", "/qui-sommes-nous",
    "/mentions-legales", "/legal", "/mentions",
    "/cgv", "/cgu",
]

def _extract_emails_from_html(html: str) -> list[str]:
    raw = _EMAIL_RE.findall(html)
    seen, result = set(), []
    for e in raw:
        el = e.lower()
        if el not in seen and not any(b in el for b in _EMAIL_BLACKLIST):
            if not el.endswith((".png",".jpg",".jpeg",".gif",".svg",".webp",".css",".js")):
                result.append(e)
                seen.add(el)
    return result

def _fetch_contact_pages(base_url: str) -> str:
    """Scrape les pages contact/about/legal pour trouver des emails."""
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    combined_html = ""
    for path in CONTACT_PATHS:
        page_url = origin + path
        try:
            if not _is_allowed_by_robots(page_url):
                continue
            with httpx.Client(headers=HEADERS, timeout=8, follow_redirects=True, verify=False) as client:
                r = client.get(page_url)
                if r.status_code < 400 and len(r.text) > 200:
                    combined_html += r.text + "\n"
        except Exception:
            continue
    return combined_html

def _check_mx_exists(domain: str) -> bool:
    """Vérifie qu'un domaine a un enregistrement MX (= peut recevoir des mails)."""
    import dns.resolver
    try:
        answers = dns.resolver.resolve(domain, "MX")
        return len(answers) > 0
    except Exception:
        return False

def _guess_common_emails(domain: str) -> str:
    """Tente les préfixes courants si le domaine a un MX."""
    try:
        if not _check_mx_exists(domain):
            return ""
    except Exception:
        return ""
    for prefix in ("contact", "info", "bonjour", "hello"):
        candidate = f"{prefix}@{domain}"
        try:
            import smtplib
            import dns.resolver
            mx = dns.resolver.resolve(domain, "MX")
            mx_host = str(sorted(mx, key=lambda r: r.preference)[0].exchange).rstrip(".")
            with smtplib.SMTP(mx_host, 25, timeout=5) as smtp:
                smtp.helo("leadsengine.app")
                smtp.mail("check@leadsengine.app")
                code, _ = smtp.rcpt(candidate)
                if code == 250:
                    return candidate
        except Exception:
            continue
    return ""

def extract_email_deep(html: str, base_url: str) -> str:
    """Extraction d'email en profondeur : page principale → pages secondaires → guess SMTP."""
    all_emails = _extract_emails_from_html(html)

    if not all_emails:
        extra_html = _fetch_contact_pages(base_url)
        if extra_html:
            all_emails = _extract_emails_from_html(html + "\n" + extra_html)

    if all_emails:
        all_emails.sort(key=lambda e: 0 if any(k in e.lower() for k in _EMAIL_PRIORITY) else 1)
        return all_emails[0]

    parsed = urlparse(base_url)
    domain = parsed.netloc.replace("www.", "")
    guessed = _guess_common_emails(domain)
    if guessed:
        return guessed

    return ""

def extract_email(html: str) -> str:
    """Fallback simple pour les appels existants sans URL."""
    all_emails = _extract_emails_from_html(html)
    if not all_emails:
        return ""
    all_emails.sort(key=lambda e: 0 if any(k in e.lower() for k in _EMAIL_PRIORITY) else 1)
    return all_emails[0]

def extract_phone(html: str) -> str:
    raw = re.findall(r"(?:(?:\+33|0033|0)\s*[\.\-\s]?)(?:[1-9]\s*(?:[\.\-\s]?\d{2}){4})", html)
    for p in raw:
        c = re.sub(r"\D","",p)
        if len(c) in (10,11): return c
    return ""


# ── Google Ads detection ──────────────────────────────────────────────────
def detect_google_ads(html: str) -> bool:
    """Détecte si le site utilise Google Ads (balises de conversion/remarketing)."""
    indicators = [
        "googleads.g.doubleclick.net",
        "googlesyndication.com",
        "google_conversion_id",
        "google_remarketing_only",
        "AW-",                          # ID de conversion Google Ads (AW-XXXXXXXXX)
        "gtag('config', 'AW-",
        "googleadservices.com",
        "google_tag_params",
        "conversion_async_click",
    ]
    html_l = html.lower()
    return any(ind.lower() in html_l for ind in indicators)


# ── Âge du domaine (RDAP) ────────────────────────────────────────────────
def get_domain_age(domain: str) -> str | None:
    """
    Récupère la date de création du domaine via RDAP (remplaçant de WHOIS).
    Retourne l'âge en format lisible ("3 ans", "8 mois") ou None.
    """
    # Extraire le domaine racine (ex: sous.domaine.fr -> domaine.fr)
    parts = domain.split(".")
    if len(parts) > 2:
        domain = ".".join(parts[-2:])

    try:
        r = httpx.get(f"https://rdap.org/domain/{domain}", timeout=10, follow_redirects=True)
        if r.status_code != 200:
            return None
        data = r.json()

        # Chercher l'événement "registration"
        for event in data.get("events", []):
            if event.get("eventAction") == "registration":
                date_str = event["eventDate"]
                created = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                now = datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
                delta = now - created
                years = delta.days // 365
                months = (delta.days % 365) // 30
                if years > 0:
                    return f"{years} an{'s' if years > 1 else ''}"
                elif months > 0:
                    return f"{months} mois"
                else:
                    return "< 1 mois"
    except Exception as e:
        log.debug("RDAP erreur pour %s : %s", domain, e)
    return None


# ── Réseaux sociaux ──────────────────────────────────────────────────────────
SOCIAL_PLATFORMS = {
    "facebook":  [r'href=["\'](?:https?://)?(?:www\.)?facebook\.com/([^"\'/\s?#]+)'],
    "instagram": [r'href=["\'](?:https?://)?(?:www\.)?instagram\.com/([^"\'/\s?#]+)'],
    "linkedin":  [r'href=["\'](?:https?://)?(?:www\.)?linkedin\.com/(?:company|in)/([^"\'/\s?#]+)'],
    "twitter":   [r'href=["\'](?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/([^"\'/\s?#]+)'],
    "youtube":   [r'href=["\'](?:https?://)?(?:www\.)?youtube\.com/(?:channel/|@|c/|user/)?([^"\'/\s?#]+)'],
    "tiktok":    [r'href=["\'](?:https?://)?(?:www\.)?tiktok\.com/@([^"\'/\s?#]+)'],
    "pinterest": [r'href=["\'](?:https?://)?(?:www\.)?pinterest\.[a-z.]+/([^"\'/\s?#]+)'],
}

SOCIAL_BLACKLIST = {"sharer", "share", "intent", "dialog", "login", "signup", "help", "about", "legal", "policy", "terms"}


def detect_social_links(html: str) -> dict:
    found = {}
    for platform, patterns in SOCIAL_PLATFORMS.items():
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for m in matches:
                handle = m.strip().rstrip("/")
                if handle.lower() not in SOCIAL_BLACKLIST and len(handle) > 1:
                    found[platform] = handle
                    break
            if platform in found:
                break
    return found


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
def compute_weaknesses(structure: dict, ps_mobile, cms: str, seo: dict, social_count: int = 0) -> str:
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
    if social_count == 0:
        w.append("Aucune présence réseaux sociaux")
    elif social_count == 1:
        w.append("Présence réseaux sociaux limitée (1 réseau)")
    for label in (seo.get("seo_missing") or []):
        w.append(label)
    return "|".join(w)


# ── Visite httpx ─────────────────────────────────────────────────────────────
def _fetch_html(url: str) -> dict:
    """
    Récupère le HTML via httpx pour la détection de CMS, emails et SEO.
    Respecte robots.txt et utilise SSL avec fallback.
    """
    result = {"html": "", "headers": {}, "final_url": url, "is_https": False}

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
        if not _is_allowed_by_robots(try_url):
            log.info("Bloqué par robots.txt : %s", try_url)
            result["_blocked_robots"] = True
            return result

        for verify_ssl in (True, False):
            try:
                with httpx.Client(
                    headers=HEADERS,
                    timeout=15,
                    follow_redirects=True,
                    verify=verify_ssl,
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
                break
            except Exception as e:
                if verify_ssl:
                    log.debug("SSL échoué %s, retry sans vérification", try_url)
                    continue
                log.debug("httpx erreur %s : %s", try_url, e)

    return result


# ── Modules par défaut ───────────────────────────────────────────────────────
DEFAULT_MODULES = {
    "cms": True, "seo": True, "contacts": True,
    "pagespeed": True, "social": True, "agence": True,
    "domain_age": True, "google_ads": True,
}


# ── Traitement d'un lead ──────────────────────────────────────────────────────
def process_lead(lead_dict: dict, modules: dict | None = None) -> dict:
    mod = {**DEFAULT_MODULES, **(modules or {})}

    url = lead_dict.get("website_url") or ""
    if not url:
        return {"_status": "skipped", "_error": "pas de site web"}
    if not url.startswith("http"):
        url = "https://" + url

    log.info("    > %s", url)

    try:
        # 1. Fetch HTML via httpx
        visit   = _fetch_html(url)
        html    = visit.get("html") or ""
        headers = visit.get("headers") or {}

        if visit.get("_blocked_robots"):
            return {"_status": "skipped", "_error": "bloqué par robots.txt"}

        if not html or len(html) < 200:
            return {"_status": "error", "_error": f"site inaccessible ({url})"}

        # 2. Parse
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            return {"_status": "error", "_error": f"parsing échoué : {e}"}

        # 3. Analyses (conditionnelles)
        domain = urlparse(visit.get("final_url") or url).netloc.replace("www.", "")

        cms     = detect_cms(html, headers) if mod["cms"] else "Inconnu"
        hosting = (get_hosting(domain) or "Inconnu") if mod["cms"] else None
        agence  = detect_agence(html) if mod["agence"] else ""
        email   = extract_email_deep(html, visit.get("final_url") or url) if mod["contacts"] else ""
        phone   = extract_phone(html) if mod["contacts"] else ""
        seo     = detect_seo(html, soup) if mod["seo"] else {}
        has_gads = detect_google_ads(html) if mod["google_ads"] else False
        dom_age = get_domain_age(domain) if mod["domain_age"] else None
        socials = detect_social_links(html) if mod["social"] else {}

        structure = analyze_structure(soup, html) if mod["seo"] else {
            "page_title": "", "has_meta_desc": False, "h1_count": 0,
            "is_responsive": False, "has_analytics": False, "copyright_year": "", "imgs_without_alt": 0,
        }

        # 4. PageSpeed (requête externe, la plus lente)
        if mod["pagespeed"]:
            try:
                ps = get_pagespeed(url)
            except Exception:
                log.warning("PageSpeed echoue pour %s", url, exc_info=True)
                ps = {"mobile": None, "desktop": None}
        else:
            ps = {"mobile": None, "desktop": None}

        # 5. Faiblesses
        weaknesses = compute_weaknesses(structure, ps.get("mobile"), cms, seo, len(socials))

        # 6. Résultat — tout converti en types SQLite-compatibles
        result = {
            "_status":  "extracted",
            "_error":   None,
            "is_https": 1 if visit.get("is_https") else 0,
        }

        if mod["contacts"]:
            result["email"] = email or None
            result["phone"] = phone or lead_dict.get("phone") or None

        if mod["cms"]:
            result["cms"]     = cms
            result["hosting"] = hosting

        if mod["agence"]:
            result["agence_web"] = agence or None

        if mod["pagespeed"]:
            result["pagespeed_mobile"]  = ps.get("mobile")
            result["pagespeed_desktop"] = ps.get("desktop")

        if mod["seo"]:
            result["page_title"]       = structure.get("page_title") or None
            result["has_meta_desc"]    = 1 if structure.get("has_meta_desc") else 0
            result["h1_count"]         = int(structure.get("h1_count") or 0)
            result["is_responsive"]    = 1 if structure.get("is_responsive") else 0
            result["has_analytics"]    = 1 if structure.get("has_analytics") else 0
            result["copyright_year"]   = structure.get("copyright_year") or None
            result["has_seo_keywords"] = 1 if seo.get("has_seo_keywords") else 0
            result["seo_signals"]      = seo.get("seo_signals") or ""
            result["seo_score"]        = int(seo.get("seo_score") or 0)

        if mod["google_ads"]:
            result["has_google_ads"] = 1 if has_gads else 0

        if mod["domain_age"]:
            result["domain_age"] = dom_age

        if mod["social"]:
            result["social_facebook"]  = socials.get("facebook")
            result["social_instagram"] = socials.get("instagram")
            result["social_linkedin"]  = socials.get("linkedin")
            result["social_twitter"]   = socials.get("twitter")
            result["social_youtube"]   = socials.get("youtube")
            result["social_tiktok"]    = socials.get("tiktok")
            result["social_pinterest"] = socials.get("pinterest")
            result["social_count"]     = len(socials)

        result["seo_weaknesses"] = weaknesses
        return result

    except Exception as e:
        log.error("process_lead exception %s : %s", url, e, exc_info=True)
        return {"_status": "error", "_error": str(e)}


# ── Agent principal ───────────────────────────────────────────────────────────
class ExtractorAgent:
    def __init__(self, queue: LeadQueue):
        self.queue = queue

    def run(self, leads: list, delay: float = 2.0, modules: dict | None = None) -> dict:
        total, success, skipped, errors = len(leads), 0, 0, 0
        active = [k for k, v in {**DEFAULT_MODULES, **(modules or {})}.items() if v]
        log.info("Agent 2 démarré — %d leads — modules: %s", total, ", ".join(active))

        for i, lead in enumerate(leads):
            lead_id = lead.get("id")
            name    = (lead.get("company_name") or "")[:40]
            log.info("[%d/%d] %s", i+1, total, name)

            if not lead_id:
                log.warning("Lead sans ID ignoré")
                errors += 1
                continue

            result  = process_lead(lead, modules)
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
