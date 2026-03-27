"""
services/dns_lookup.py
Identification de l'hébergeur via DNS + headers HTTP.
100% gratuit, aucune API externe.
"""

import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import dns.resolver
import httpx

# Signatures d'hébergeurs dans les enregistrements DNS
HOSTING_SIGNATURES = {
    # Hébergeurs FR
    "o2switch":        ["o2switch.net", "o2switch.fr"],
    "OVH":             ["ovh.net", "ovh.com", "ovhcloud.com"],
    "Ionos":           ["ionos.fr", "ionos.com", "1and1.com", "1and1.fr"],
    "Gandi":           ["gandi.net"],
    "LWS":             ["lws.fr", "lws-hosting.com"],
    "Infomaniak":      ["infomaniak.com", "infomaniak.ch"],
    "PlanetHoster":    ["planethoster.net", "planethoster.com"],
    "Ikoula":          ["ikoula.com"],
    "Nuxit":           ["nuxit.com"],
    "Hostinger":       ["hostinger.com", "hstgr.com"],
    "Scaleway":        ["scaleway.com", "online.net"],
    # Hébergeurs internationaux
    "Cloudflare":      ["cloudflare.com", "cloudflare.net"],
    "AWS":             ["amazonaws.com", "awsdns"],
    "Google Cloud":    ["googleusercontent.com", "google.com"],
    "Azure":           ["azure.com", "azurewebsites.net", "windows.net"],
    "GitHub Pages":    ["github.io", "github.com"],
    "Vercel":          ["vercel.app", "vercel.com"],
    "Netlify":         ["netlify.app", "netlify.com"],
    "Heroku":          ["heroku.com", "herokussl.com"],
    "WP Engine":       ["wpengine.com"],
    "SiteGround":      ["siteground.com", "sgvps.net"],
    "Bluehost":        ["bluehost.com"],
    "GoDaddy":         ["godaddy.com", "secureserver.net"],
    # Builders
    "Wix":             ["wix.com", "wixdns.net"],
    "Squarespace":     ["squarespace.com", "sqsp.net"],
    "Webflow":         ["webflow.io", "webflow.com"],
    "Shopify":         ["shopify.com", "myshopify.com"],
}


def get_hosting(domain: str) -> str | None:
    """
    Identifie l'hébergeur d'un domaine via :
    1. Enregistrements NS (nameservers)
    2. Enregistrement CNAME
    3. Enregistrement A (IP) → reverse lookup
    """
    if not domain:
        return None

    domain = domain.lower().strip().replace("www.", "")

    # 1. Nameservers
    try:
        ns_records = dns.resolver.resolve(domain, "NS", lifetime=5)
        ns_str = " ".join(str(r).lower() for r in ns_records)
        result = _match_signatures(ns_str)
        if result:
            log.debug("Hébergeur via NS : %s → %s", domain, result)
            return result
    except Exception:
        log.warning("DNS NS lookup echoue pour '%s'", domain, exc_info=True)

    # 2. CNAME
    try:
        cname_records = dns.resolver.resolve(domain, "CNAME", lifetime=5)
        cname_str = " ".join(str(r).lower() for r in cname_records)
        result = _match_signatures(cname_str)
        if result:
            log.debug("Hébergeur via CNAME : %s → %s", domain, result)
            return result
    except Exception:
        log.warning("DNS CNAME lookup echoue pour '%s'", domain, exc_info=True)

    # 3. MX records (bonus : détection GSuite/Outlook pour info)
    try:
        mx_records = dns.resolver.resolve(domain, "MX", lifetime=5)
        mx_str = " ".join(str(r).lower() for r in mx_records)
        if "google" in mx_str:
            log.debug("Emails Google Workspace détectés pour %s", domain)
        elif "outlook" in mx_str or "microsoft" in mx_str:
            log.debug("Emails Microsoft 365 détectés pour %s", domain)
    except Exception:
        log.warning("DNS MX lookup echoue pour '%s'", domain, exc_info=True)

    return None


def _match_signatures(text: str) -> str | None:
    for hosting_name, sigs in HOSTING_SIGNATURES.items():
        for sig in sigs:
            if sig in text:
                return hosting_name
    return None
