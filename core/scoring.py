"""
core/scoring.py — Scoring automatique des leads (0-100).
Calculé à partir des données déjà scrapées, sans API externe.
"""

import pandas as pd


def compute_lead_score(row: pd.Series) -> int:
    score = 0

    # Contact direct
    email = row.get("email")
    if pd.notna(email) and str(email).strip():
        score += 15
    phone = row.get("phone")
    if pd.notna(phone) and str(phone).strip():
        score += 10

    # Présence en ligne
    website = row.get("website_url")
    if pd.notna(website) and str(website).strip():
        score += 10

    # Réputation Google
    rating = row.get("google_rating")
    if pd.notna(rating):
        try:
            if float(rating) >= 4.0:
                score += 10
        except (ValueError, TypeError):
            pass

    reviews = row.get("review_count")
    if pd.notna(reviews):
        try:
            if int(reviews) > 10:
                score += 5
        except (ValueError, TypeError):
            pass

    # Opportunités digitales (scoring inversé = besoin = opportunité)
    seo = row.get("seo_score")
    if pd.notna(seo):
        try:
            if int(seo) < 5:
                score += 15
            elif int(seo) < 7:
                score += 8
        except (ValueError, TypeError):
            pass

    ads = row.get("has_google_ads")
    if pd.notna(ads) and str(ads) in ("0", "False", "false", ""):
        score += 10

    cms = row.get("cms")
    if pd.notna(cms) and str(cms).strip():
        score += 5

    speed = row.get("pagespeed_mobile")
    if pd.notna(speed):
        try:
            if int(speed) < 50:
                score += 10
        except (ValueError, TypeError):
            pass

    # Entreprise structurée
    effectif = row.get("employee_range")
    if pd.notna(effectif) and str(effectif).strip():
        score += 5

    dirigeant = row.get("owner_name")
    if pd.notna(dirigeant) and str(dirigeant).strip():
        score += 5

    return min(score, 100)


def score_label(score: int) -> tuple[str, str]:
    """Retourne (emoji, couleur hex) pour un score donné."""
    if score >= 60:
        return "🟢", "#2ECC71"
    if score >= 30:
        return "🟡", "#F5D87A"
    return "🔴", "#E74C3C"
