"""
core/scoring.py — Scoring objectif basé sur la complétude des données (0-100).
Un lead qualifié = un lead dont la fiche est complète, pas un jugement de valeur.
"""

import pandas as pd


# Chaque champ rempli contribue au score. Les poids reflètent l'importance
# pour la complétude d'une fiche lead exploitable commercialement.
COMPLETENESS_FIELDS = {
    # ── Contact (30 pts) ─────────────────────────────────────
    "email":            12,
    "phone":            10,
    "owner_name":        5,
    "contact_name":      3,
    # ── Entreprise (25 pts) ──────────────────────────────────
    "company_name":      3,
    "city":              3,
    "sector":            3,
    "siren":             4,
    "legal_form":        3,
    "employee_range":    3,
    "creation_date":     3,
    "capital_social":    3,
    # ── Présence web (25 pts) ────────────────────────────────
    "website_url":       5,
    "cms":               3,
    "seo_score":         3,
    "pagespeed_mobile":  3,
    "has_analytics":     2,
    "is_responsive":     2,
    "has_google_ads":    2,
    "domain_age":        3,
    "hosting":           2,
    # ── Réputation & réseaux (20 pts) ────────────────────────
    "google_rating":     5,
    "review_count":      5,
    "social_count":      5,
    "agence_web":        2,
    "gmb_confirmed":     3,
}

_TOTAL_WEIGHT = sum(COMPLETENESS_FIELDS.values())


def _field_filled(row: pd.Series, field: str) -> bool:
    val = row.get(field)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    s = str(val).strip()
    if s in ("", "0", "Inconnu", "None", "nan"):
        return False
    if field == "cms" and s == "Inconnu":
        return False
    if field == "social_count":
        try:
            return int(float(s)) > 0
        except (ValueError, TypeError):
            return False
    if field in ("has_analytics", "is_responsive", "has_google_ads", "gmb_confirmed"):
        try:
            return int(float(s)) == 1
        except (ValueError, TypeError):
            return False
    if field == "seo_score":
        try:
            return int(float(s)) > 0
        except (ValueError, TypeError):
            return False
    if field in ("google_rating", "review_count", "pagespeed_mobile"):
        try:
            return float(s) > 0
        except (ValueError, TypeError):
            return False
    return True


def compute_lead_score(row: pd.Series) -> int:
    points = sum(
        weight for field, weight in COMPLETENESS_FIELDS.items()
        if _field_filled(row, field)
    )
    return round(points * 100 / _TOTAL_WEIGHT)


def score_label(score: int) -> tuple[str, str]:
    """Retourne (emoji, couleur hex) pour un score donné."""
    if score >= 60:
        return "🟢", "#2ECC71"
    if score >= 30:
        return "🟡", "#F5D87A"
    return "🔴", "#E74C3C"
