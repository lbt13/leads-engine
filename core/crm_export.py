"""
core/crm_export.py — Export CSV formaté pour chaque CRM.
Chaque CRM n'exporte que ses champs natifs reconnus à l'import.
Les données supplémentaires sont regroupées dans un champ notes/description.
"""

import io
import csv
import pandas as pd


# ── Mappings CRM ─────────────────────────────────────────────────────────────
# "columns"     = uniquement les champs natifs du CRM (auto-mapping garanti)
# "extra_cols"  = nos champs supplémentaires à regrouper dans le champ notes
# "notes_field" = nom du champ natif qui recevra les données extra

CRM_MAPPINGS = {
    # ── HubSpot ──────────────────────────────────────────────────────────────
    # Doc: https://knowledge.hubspot.com/import-and-export/set-up-your-import-file
    "hubspot": {
        "label": "HubSpot",
        "description": "Import Contacts/Entreprises HubSpot",
        "notes_field": "Notes",
        "columns": {
            "company_name":     "Company name",
            "owner_name":       "First name",
            "email":            "Email",
            "phone":            "Phone number",
            "website_url":      "Company domain name",
            "address":          "Street address",
            "city":             "City",
            "siege_cp":         "Zip",
            "sector":           "Industry",
            "employee_range":   "Number of employees",
            "owner_role":       "Job title",
        },
        "extra_cols": [
            ("google_rating",    "Note Google"),
            ("review_count",     "Avis Google"),
            ("siren",            "SIREN"),
            ("siret",            "SIRET"),
            ("legal_form",       "Forme juridique"),
            ("creation_date",    "Date création"),
            ("cms",              "CMS"),
            ("hosting",          "Hébergeur"),
            ("pagespeed_mobile", "Vitesse mobile"),
            ("seo_score",        "Score SEO"),
            ("seo_weaknesses",   "Faiblesses SEO"),
            ("naf_label",        "Activité NAF"),
            ("domain_age",       "Âge domaine"),
            ("has_google_ads",   "Google Ads"),
        ],
    },
    # ── Salesforce ───────────────────────────────────────────────────────────
    # Doc: https://help.salesforce.com/s/articleView?id=sales.leads_fields.htm
    # Champs natifs Lead : Company, Last Name, Email, Phone, Website,
    # Street, City, Zip/Postal Code, Industry, No. of Employees, Title, Description
    "salesforce": {
        "label": "Salesforce",
        "description": "Import Leads Salesforce",
        "notes_field": "Description",
        "columns": {
            "company_name":     "Company",
            "owner_name":       "Last Name",
            "email":            "Email",
            "phone":            "Phone",
            "website_url":      "Website",
            "address":          "Street",
            "city":             "City",
            "siege_cp":         "Zip/Postal Code",
            "sector":           "Industry",
            "employee_range":   "No. of Employees",
            "owner_role":       "Title",
        },
        "extra_cols": [
            ("google_rating",    "Note Google"),
            ("review_count",     "Avis Google"),
            ("siren",            "SIREN"),
            ("siret",            "SIRET"),
            ("legal_form",       "Forme juridique"),
            ("creation_date",    "Date création"),
            ("cms",              "CMS"),
            ("seo_score",        "Score SEO"),
            ("seo_weaknesses",   "Faiblesses SEO"),
            ("naf_label",        "Activité NAF"),
            ("domain_age",       "Âge domaine"),
            ("has_google_ads",   "Google Ads"),
        ],
    },
    # ── Pipedrive ────────────────────────────────────────────────────────────
    # Doc: https://support.pipedrive.com/en/article/importing-mandatory-fields
    "pipedrive": {
        "label": "Pipedrive",
        "description": "Import Organisations/Personnes Pipedrive",
        "notes_field": "Person - Note",
        "columns": {
            "company_name":     "Organization - Name",
            "owner_name":       "Person - Name",
            "email":            "Person - Email",
            "phone":            "Person - Phone",
            "website_url":      "Organization - Address",
            "address":          "Organization - Address",
            "city":             "Organization - Address locality",
            "siege_cp":         "Organization - Address postal code",
            "owner_role":       "Person - Job title",
        },
        "extra_cols": [
            ("google_rating",    "Note Google"),
            ("review_count",     "Avis Google"),
            ("sector",           "Secteur"),
            ("siren",            "SIREN"),
            ("siret",            "SIRET"),
            ("employee_range",   "Effectif"),
            ("creation_date",    "Date création"),
            ("cms",              "CMS"),
            ("seo_score",        "Score SEO"),
            ("seo_weaknesses",   "Faiblesses SEO"),
            ("naf_label",        "Activité NAF"),
            ("domain_age",       "Âge domaine"),
            ("has_google_ads",   "Google Ads"),
        ],
    },
    # ── Monday.com ───────────────────────────────────────────────────────────
    # Doc: https://support.monday.com/hc/en-us/articles/360000219209
    # Pas de noms imposés — colonnes CSV mappées aux colonnes du board
    "monday": {
        "label": "Monday.com",
        "description": "Import tableau Monday.com",
        "notes_field": "Notes",
        "columns": {
            "company_name":     "Name",
            "owner_name":       "Contact",
            "email":            "Email",
            "phone":            "Phone",
            "website_url":      "Website",
            "address":          "Address",
            "city":             "City",
            "sector":           "Industry",
            "employee_range":   "Employees",
            "owner_role":       "Role",
        },
        "extra_cols": [
            ("google_rating",    "Note Google"),
            ("review_count",     "Avis Google"),
            ("siren",            "SIREN"),
            ("siret",            "SIRET"),
            ("creation_date",    "Date création"),
            ("cms",              "CMS"),
            ("seo_score",        "Score SEO"),
            ("seo_weaknesses",   "Faiblesses SEO"),
            ("naf_label",        "Activité NAF"),
            ("domain_age",       "Âge domaine"),
            ("has_google_ads",   "Google Ads"),
        ],
    },
    # ── Notion ───────────────────────────────────────────────────────────────
    # Pas de format imposé — colonnes CSV deviennent propriétés de la base
    "notion": {
        "label": "Notion",
        "description": "Import base de données Notion",
        "notes_field": "Notes",
        "columns": {
            "company_name":     "Entreprise",
            "owner_name":       "Dirigeant",
            "owner_role":       "Rôle",
            "email":            "Email",
            "phone":            "Téléphone",
            "website_url":      "Site web",
            "address":          "Adresse",
            "city":             "Ville",
            "siege_cp":         "Code postal",
            "sector":           "Secteur",
            "google_rating":    "Note Google",
            "review_count":     "Avis Google",
            "siren":            "SIREN",
            "siret":            "SIRET",
            "legal_form":       "Forme juridique",
            "employee_range":   "Effectif",
            "creation_date":    "Date création",
            "cms":              "CMS",
            "hosting":          "Hébergeur",
            "pagespeed_mobile": "Vitesse mobile",
            "seo_score":        "Score SEO",
            "seo_weaknesses":   "Faiblesses SEO",
            "naf_label":        "Activité NAF",
            "domain_age":       "Âge domaine",
            "has_google_ads":   "Google Ads",
            "etat":             "Statut",
        },
        "extra_cols": [],
    },
    # ── Airtable ─────────────────────────────────────────────────────────────
    # Pas de format imposé — colonnes CSV deviennent champs de la table
    "airtable": {
        "label": "Airtable",
        "description": "Import base Airtable",
        "notes_field": "Notes",
        "columns": {
            "company_name":     "Name",
            "owner_name":       "Contact Name",
            "email":            "Email",
            "phone":            "Phone",
            "website_url":      "Website",
            "address":          "Address",
            "city":             "City",
            "siege_cp":         "Postal Code",
            "sector":           "Industry",
            "google_rating":    "Google Rating",
            "review_count":     "Reviews",
            "siren":            "SIREN",
            "siret":            "SIRET",
            "employee_range":   "Employees",
            "creation_date":    "Founded",
            "owner_role":       "Job Title",
            "cms":              "CMS",
            "hosting":          "Hosting",
            "seo_score":        "SEO Score",
            "seo_weaknesses":   "SEO Issues",
            "naf_label":        "Activity",
            "domain_age":       "Domain Age",
            "has_google_ads":   "Google Ads",
        },
        "extra_cols": [],
    },
}


def get_crm_list() -> list[dict]:
    """Retourne la liste des CRM disponibles avec label et description."""
    return [
        {"key": k, "label": v["label"], "description": v["description"]}
        for k, v in CRM_MAPPINGS.items()
    ]


def _build_notes(row: pd.Series, extra_cols: list[tuple[str, str]]) -> str:
    """Construit un texte récapitulatif des données supplémentaires."""
    parts = []
    for col, label in extra_cols:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() not in ("", "0", "None"):
            if col == "has_google_ads":
                val = "Oui" if str(val) in ("1", "True", "true") else "Non"
            parts.append(f"{label}: {val}")
    return " | ".join(parts)


def export_crm_csv(df: pd.DataFrame, crm_key: str) -> bytes:
    """
    Exporte un DataFrame au format CSV prêt à importer dans le CRM choisi.
    Seuls les champs natifs du CRM sont exportés en colonnes.
    Les données supplémentaires sont regroupées dans le champ notes/description.
    Retourne les bytes du CSV (UTF-8 BOM pour Excel/import).
    """
    mapping = CRM_MAPPINGS.get(crm_key)
    if not mapping:
        raise ValueError(f"CRM inconnu : {crm_key}")

    columns = mapping["columns"]
    extra = mapping.get("extra_cols", [])
    notes_field = mapping.get("notes_field", "Notes")

    # Colonnes natives présentes dans le DataFrame
    cols_present = [c for c in columns if c in df.columns]
    df_export = df[cols_present].rename(columns=columns).copy()

    # Ajoute la colonne notes si on a des champs extra
    if extra:
        df_export[notes_field] = df.apply(
            lambda row: _build_notes(row, extra), axis=1
        )

    buf = io.StringIO()
    df_export.to_csv(buf, index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
    return ("\ufeff" + buf.getvalue()).encode("utf-8")
