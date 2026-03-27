import sys, asyncio, sqlite3, io, threading, re, time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import nest_asyncio
nest_asyncio.apply()

# ROOT  = dossier des données (leads.db, crm/, .env…)
#         → %APPDATA%\LeadsEngine en exe, dossier source en dev
# _CODE = dossier du code Python (pour sys.path)
#         → _MEIPASS en exe (géré par PyInstaller), dossier source en dev
import os as _os
ROOT  = Path(_os.environ.get("LEADS_ENGINE_ROOT", str(Path(__file__).parent)))
_CODE = Path(getattr(sys, "_MEIPASS", str(ROOT)))
if str(_CODE) not in sys.path:
    sys.path.insert(0, str(_CODE))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from core.logger import get_logger, setup_logging
setup_logging()
log = get_logger("app")

from config import config
from core.queue import LeadQueue
from core.crm_filter import crm_stats, CRM_DIR, compare_against_crm, parse_crm_file
from agents.scraper import ScraperAgent
from agents.extractor import ExtractorAgent

# Crée les dossiers et la DB dès le démarrage — garantit que tout existe
# indépendamment de si un scraping a déjà été fait ou non.
(ROOT / "crm").mkdir(parents=True, exist_ok=True)
(ROOT / "analyses_a2").mkdir(parents=True, exist_ok=True)
LeadQueue(str(ROOT / config.db_path))  # initialise leads.db si absent

st.set_page_config(page_title="Leads Engine", page_icon="◈", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Epilogue:wght@300;400;500;600;700;800;900&family=IBM+Plex+Mono:wght@300;400;500&display=swap');
*,*::before,*::after{box-sizing:border-box}
html,body,[class*="css"],.stApp{font-family:'Epilogue',sans-serif !important;background-color:#0D0E11 !important;color:#E2DDD6 !important}
[data-testid="stSidebar"],[data-testid="collapsedControl"]{display:none !important}
.main .block-container{padding:0 !important;max-width:100% !important}
h1{font-family:'Epilogue',sans-serif !important;font-size:26px !important;font-weight:800 !important;letter-spacing:-0.8px !important;color:#F0EBE3 !important;margin-bottom:4px !important;line-height:1.1 !important}
h2{font-family:'Epilogue',sans-serif !important;font-size:15px !important;font-weight:700 !important;color:#C8C2BB !important}
[data-testid="stMetric"]{background:#13151A !important;border:1px solid #1E2028 !important;border-radius:8px !important;padding:14px 18px !important}
[data-testid="stMetric"]:hover{border-color:#2E3240 !important}
[data-testid="stMetricLabel"]{font-size:10px !important;font-weight:600 !important;text-transform:uppercase !important;letter-spacing:1.2px !important;color:#4A4D58 !important}
[data-testid="stMetricValue"]{font-family:'IBM Plex Mono',monospace !important;font-size:24px !important;font-weight:500 !important;color:#F0EBE3 !important;line-height:1.2 !important}
.stTextInput>div>div>input,.stSelectbox>div>div>div,.stMultiSelect>div>div>div{background-color:#13151A !important;border:1px solid #1E2028 !important;border-radius:6px !important;color:#E2DDD6 !important;font-family:'Epilogue',sans-serif !important}
.stTextInput>div>div>input:focus{border-color:#E87B2A !important;box-shadow:0 0 0 2px rgba(232,123,42,.12) !important}
.stTextInput label,.stSelectbox label,.stSlider label,.stMultiSelect label{font-size:10px !important;font-weight:700 !important;text-transform:uppercase !important;letter-spacing:1.2px !important;color:#4A4D58 !important}
.stButton>button[kind="primary"]{background:#E87B2A !important;color:#0D0E11 !important;border:none !important;border-radius:6px !important;font-family:'Epilogue',sans-serif !important;font-weight:700 !important;font-size:13px !important}
.stButton>button[kind="primary"]:hover{opacity:.88 !important}
.stButton>button[kind="secondary"]{background:#13151A !important;color:#C8C2BB !important;border:1px solid #1E2028 !important;border-radius:6px !important;font-family:'Epilogue',sans-serif !important;font-weight:600 !important;font-size:13px !important}
.stButton>button[kind="secondary"]:hover{border-color:#E87B2A !important;color:#E87B2A !important}
[data-testid="stDownloadButton"] button{background:#13151A !important;color:#C8C2BB !important;border:1px solid #1E2028 !important;border-radius:6px !important;font-family:'Epilogue',sans-serif !important;font-weight:600 !important;font-size:13px !important}
[data-testid="stDownloadButton"] button:hover{border-color:#E87B2A !important;color:#E87B2A !important}
[data-testid="stDataFrame"]{border:1px solid #1E2028 !important;border-radius:8px !important;overflow:hidden !important}
[data-testid="stDataFrame"] th{background-color:#13151A !important;color:#4A4D58 !important;font-family:'IBM Plex Mono',monospace !important;font-size:10px !important;text-transform:uppercase !important;letter-spacing:1px !important}
[data-testid="stDataFrame"] td{color:#C8C2BB !important;font-family:'IBM Plex Mono',monospace !important;font-size:12px !important;background-color:#0D0E11 !important}
[data-testid="stDataFrame"] tr:hover td{background-color:#13151A !important}
.stSuccess{background:#0A1A10 !important;border:1px solid #1A4228 !important;border-left:3px solid #2ECC71 !important;border-radius:6px !important;color:#7DFAB8 !important}
.stError{background:#1A0A0A !important;border:1px solid #4A1A1A !important;border-left:3px solid #E84C4C !important;border-radius:6px !important}
.stInfo{background:#0D0F1A !important;border:1px solid #1A1E3A !important;border-left:3px solid #4A7CFF !important;border-radius:6px !important;color:#8FA8FF !important}
.stWarning{background:#1A1200 !important;border:1px solid #3A2E00 !important;border-left:3px solid #E8C32A !important;border-radius:6px !important;color:#F5D87A !important}
.stToggle label,.stCheckbox label{color:#C8C2BB !important;font-size:14px !important}
.stProgress>div>div{background:#E87B2A !important;border-radius:2px !important}
.stCaption{color:#4A4D58 !important;font-family:'IBM Plex Mono',monospace !important;font-size:11px !important}
hr{border-color:#1E2028 !important;margin:20px 0 !important}
.stExpander{border:1px solid #1E2028 !important;border-radius:8px !important;background:#13151A !important}
[data-testid="stTabs"] [role="tablist"]{border-bottom:1px solid #1E2028 !important;gap:0 !important;background:transparent !important;padding:0 40px !important}
[data-testid="stTabs"] button[role="tab"]{font-family:'Epilogue',sans-serif !important;font-size:13px !important;font-weight:600 !important;color:#4A4D58 !important;padding:14px 22px !important;border-radius:0 !important;border-bottom:2px solid transparent !important;background:transparent !important}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"]{color:#F0EBE3 !important;border-bottom:2px solid #E87B2A !important}
[data-testid="stTabs"] [role="tabpanel"]{padding:28px 40px 0 !important}
.section-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:#4A4D58;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #1E2028}
.a2-banner{background:linear-gradient(135deg,#13151A 0%,#1A1008 100%);border:1px solid #2E2010;border-left:3px solid #E87B2A;border-radius:8px;padding:14px 18px;margin-bottom:18px}
.a2-banner .t{font-size:13px;font-weight:700;color:#E87B2A;margin-bottom:2px}
.a2-banner .d{font-size:12px;color:#6A6560}
.export-box{margin-top:20px;padding:18px 22px;background:#13151A;border:1px solid #1E2028;border-radius:8px}
.export-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:#4A4D58;margin-bottom:14px}
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_df(session_id=None):
    db = ROOT / config.db_path
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(db)
    q = ("SELECT * FROM leads WHERE session_id=? ORDER BY google_rating DESC"
         if session_id else "SELECT * FROM leads ORDER BY scraped_at DESC")
    df = pd.read_sql(q, conn, params=(session_id,) if session_id else None)
    conn.close()
    return df


def _autofit(writer):
    for sh in writer.sheets.values():
        for col in sh.columns:
            sh.column_dimensions[col[0].column_letter].width = min(
                max(len(str(c.value or "")) for c in col) + 3, 38)


def to_excel_a1(df_a1) -> bytes:
    """Export Agent 1 uniquement — un seul onglet."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_a1.to_excel(w, index=False, sheet_name="Leads")
        _autofit(w)
    return buf.getvalue()


def to_excel_combined(df, cols_a1: dict, cols_a2: dict) -> bytes:
    """
    Export Agent 1 + Agent 2 fusionnés dans un seul onglet.
    Toutes les colonnes des deux agents côte à côte sur chaque ligne.
    """
    # Colonnes A1 + A2 sans doublons (A1 en premier)
    all_cols = {**cols_a1, **{k: v for k, v in cols_a2.items() if k not in cols_a1}}
    cols_present = [c for c in all_cols if c in df.columns]
    df_export = df[cols_present].rename(columns=all_cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_export.to_excel(w, index=False, sheet_name="Leads complets")
        _autofit(w)
    return buf.getvalue()


def to_excel_qualifies(df_qual, cols_a1: dict, cols_a2: dict) -> bytes:
    """Export leads qualifiés avec toutes les colonnes A1 + A2."""
    all_cols = {**cols_a1, **{k: v for k, v in cols_a2.items() if k not in cols_a1}}
    cols_present = [c for c in all_cols if c in df_qual.columns]
    df_export = df_qual[cols_present].rename(columns=all_cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_export.to_excel(w, index=False, sheet_name="Leads qualifiés")
        _autofit(w)
    return buf.getvalue()


def to_excel_multi(df_a1, df_a2=None, df_qual=None):
    """Conservé pour compatibilité — utilisé dans l'onglet global."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_a1.to_excel(w, index=False, sheet_name="Infos générales")
        if df_a2 is not None and not df_a2.empty:
            df_a2.to_excel(w, index=False, sheet_name="Technique site web")
        if df_qual is not None and not df_qual.empty:
            df_qual.to_excel(w, index=False, sheet_name="Leads qualifiés")
        _autofit(w)
    return buf.getvalue()


def to_excel_vendeur(df) -> bytes:
    """Export vue vendeur — thème clair professionnel, épuré."""
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side, GradientFill
    from openpyxl.utils import get_column_letter

    cols_present = [c for c in COLS_VENDEUR if c in df.columns]
    df_exp = df[cols_present].rename(columns=COLS_VENDEUR).copy()

    # Styles — thème sombre
    fill_header  = PatternFill("solid", fgColor="E87B2A")
    fill_row_a   = PatternFill("solid", fgColor="0D0E11")
    fill_row_b   = PatternFill("solid", fgColor="13151A")
    font_header  = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
    font_company = Font(name="Calibri", bold=True, color="F0EBE3", size=10)
    font_normal  = Font(name="Calibri", color="C8C2BB", size=10)
    font_seo_ok  = Font(name="Calibri", bold=True, color="2ECC71", size=10)
    font_seo_mid = Font(name="Calibri", bold=True, color="F39C12", size=10)
    font_seo_bad = Font(name="Calibri", bold=True, color="E74C3C", size=10)
    font_weak    = Font(name="Calibri", color="E87B2A", size=10)
    font_link    = Font(name="Calibri", color="5DADE2", underline="single", size=10)
    border_cell  = Border(
        top=Side(style="thin", color="1E2028"),
        bottom=Side(style="thin", color="1E2028"),
        left=Side(style="thin", color="1E2028"),
        right=Side(style="thin", color="1E2028"),
    )

    col_names   = list(df_exp.columns)
    seo_col     = col_names.index("Score SEO /10")  + 1 if "Score SEO /10"  in col_names else None
    weak_col    = col_names.index("Faiblesses SEO") + 1 if "Faiblesses SEO" in col_names else None
    name_col    = col_names.index("Entreprise")     + 1 if "Entreprise"     in col_names else None
    site_col    = col_names.index("Site web")       + 1 if "Site web"       in col_names else None

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_exp.to_excel(w, index=False, sheet_name="Leads")
        ws = w.sheets["Leads"]

        # En-tête
        ws.row_dimensions[1].height = 22
        for cell in ws[1]:
            cell.fill      = fill_header
            cell.font      = font_header
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border    = border_cell

        # Données
        for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
            fill = fill_row_a if row_idx % 2 == 0 else fill_row_b
            ws.row_dimensions[row_idx].height = 18
            for cell in row:
                cell.fill      = fill
                cell.font      = font_normal
                cell.alignment = Alignment(vertical="center", wrap_text=False)
                cell.border    = border_cell

            # Entreprise en gras
            if name_col:
                ws.cell(row=row_idx, column=name_col).font = font_company

            # Site web en lien
            if site_col:
                c = ws.cell(row=row_idx, column=site_col)
                if c.value:
                    c.font = font_link

            # Score SEO coloré
            if seo_col:
                c = ws.cell(row=row_idx, column=seo_col)
                if isinstance(c.value, (int, float)):
                    c.font = font_seo_ok if c.value >= 7 else (font_seo_mid if c.value >= 4 else font_seo_bad)
                    c.alignment = Alignment(horizontal="center", vertical="center")

            # Faiblesses en rouge discret
            if weak_col:
                c = ws.cell(row=row_idx, column=weak_col)
                if c.value:
                    c.font      = font_weak
                    c.alignment = Alignment(vertical="center", wrap_text=True)

        # Largeurs colonnes
        FIXED_WIDTHS = {
            "Entreprise": 28, "Ville": 16, "Secteur": 18, "Dirigeant": 20,
            "Rôle": 16, "Téléphone": 14, "Email": 26, "Site web": 30,
            "Note Google": 12, "Avis": 8, "Forme juridique": 16, "Effectif": 12,
            "CMS": 12, "Hébergeur": 14, "Vitesse mobile": 14,
            "Score SEO /10": 13, "Faiblesses SEO": 42, "Création": 12,
            "État": 10, "SIREN": 12, "Adresse": 28,
        }
        for col in ws.columns:
            header = ws.cell(row=1, column=col[0].column).value
            width  = FIXED_WIDTHS.get(header, 16)
            ws.column_dimensions[get_column_letter(col[0].column)].width = width

        # Figer en-tête + tab name
        ws.freeze_panes   = "A2"
        ws.sheet_view.showGridLines = False

    return buf.getvalue()


def fmt_session(s):
    return s.get("session_label") or s.get("session_id") or "—"


def is_qualifie(row) -> bool:
    """
    Lead qualifié = les 10 critères suivants sont tous remplis :
    nom, adresse, téléphone, ville, secteur,
    dirigeant, CMS, hébergeur, vitesse du site, présence/absence SEO keywords
    """
    adresse = row.get("address") or row.get("siege_adresse")
    return all([
        row.get("company_name"),
        adresse,
        row.get("phone"),
        row.get("city"),
        row.get("sector"),
        row.get("owner_name"),
        row.get("cms"),
        row.get("hosting"),
        row.get("pagespeed_mobile") is not None,
        row.get("has_seo_keywords") is not None,
    ])


def has_agent2(df):
    return "cms" in df.columns and df["cms"].notna().any()


def db_stats():
    db = ROOT / config.db_path
    if not db.exists():
        return {}
    try:
        conn = sqlite3.connect(db)
        r = {
            "total":    conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0],
            "sessions": conn.execute("SELECT COUNT(DISTINCT session_id) FROM leads").fetchone()[0],
            "analyses": conn.execute("SELECT COUNT(*) FROM leads WHERE status='extracted'").fetchone()[0],
            "qualifies": conn.execute("""
                SELECT COUNT(*) FROM leads
                WHERE company_name IS NOT NULL
                AND (address IS NOT NULL OR siege_adresse IS NOT NULL)
                AND phone IS NOT NULL AND city IS NOT NULL AND sector IS NOT NULL
                AND owner_name IS NOT NULL AND cms IS NOT NULL AND hosting IS NOT NULL
                AND pagespeed_mobile IS NOT NULL AND has_seo_keywords IS NOT NULL
            """).fetchone()[0],
        }
        conn.close()
        return r
    except Exception:
        log.warning("Calcul stats DB echoue", exc_info=True)
        return {}


ANALYSES_A2_DIR = ROOT / "analyses_a2"
ANALYSES_A2_DB  = ANALYSES_A2_DIR / "analyses.db"


def _enrich_websites(sid: str, db_path: str, delay: float, result_holder: list, error_holder: list):
    """
    Cherche les sites web manquants via Google Maps (SerpAPI).
    Regroupe les entreprises par (secteur, ville) et fait UNE requête par groupe
    (même logique que le scraping) — beaucoup plus économe en crédits.
    """
    import re as _re
    from difflib import SequenceMatcher
    from services.serpapi import search_google_maps

    def _norm(s):
        return _re.sub(r"\s+", " ", _re.sub(r"[^\w\s]", "", str(s or "").lower())).strip()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        leads = [dict(r) for r in conn.execute(
            "SELECT id, company_name, city, sector FROM leads WHERE session_id=? AND (website_url IS NULL OR website_url='')",
            (sid,)
        ).fetchall()]
        conn.close()

        total = len(leads)
        found = 0

        # Regrouper par (secteur, ville) — une seule requête Maps par groupe
        # On retire le code postal (ex: "13100 Aix-en-Provence" → "aix en provence")
        def _city_key(city: str) -> str:
            return _norm(_re.sub(r"^\d{4,5}\s*", "", str(city or "")).strip())

        groups: dict[tuple, list] = {}
        for lead in leads:
            key = (_norm(lead.get("sector") or ""), _city_key(lead.get("city") or ""))
            groups.setdefault(key, []).append(lead)

        for (sector, city), group in groups.items():
            # Ignorer les groupes sans ville ou trop petits (< 3 entreprises)
            # — peu de chance de trouver les bonnes entreprises sans localisation précise
            if not city or len(group) < 3:
                continue

            # Construire le lookup nom → id pour ce groupe
            lookup = {_norm(l["company_name"]): l for l in group}

            query = sector or " ".join(l["company_name"] for l in group[:3])
            if not query:
                continue

            try:
                for raw in search_google_maps(query, city, max_results=20):
                    result_name = raw.get("title", "")
                    w = (raw.get("website") or raw.get("links", {}).get("website") or "").strip().rstrip("/")
                    if not w:
                        continue
                    # Chercher la meilleure correspondance dans ce groupe
                    best_key, best_ratio = None, 0.0
                    for k in lookup:
                        r = SequenceMatcher(None, _norm(result_name), k).ratio()
                        if r > best_ratio:
                            best_ratio, best_key = r, k
                    if best_ratio >= 0.55 and best_key:
                        lead_match = lookup.pop(best_key)
                        c2 = sqlite3.connect(db_path)
                        c2.execute("UPDATE leads SET website_url=?, status='scraped' WHERE id=?",
                                   (w, lead_match["id"]))
                        c2.commit()
                        c2.close()
                        found += 1
                    if not lookup:
                        break  # tous les leads du groupe sont appariés
            except Exception:
                log.warning("Enrichissement site web echoue pour un groupe", exc_info=True)

            time.sleep(delay)

        result_holder.append((found, total))
    except Exception as e:
        log.error("_enrich_websites erreur globale", exc_info=True)
        error_holder.append(e)


def _find_db_duplicates() -> tuple[int, int]:
    """
    Analyse leads.db et supprime les doublons (SIREN exact ou nom+ville ≥ 85%).
    Conserve l'entrée la plus riche pour chaque groupe.
    Retourne (nb_groupes, nb_supprimés).
    """
    import re as _re
    from difflib import SequenceMatcher

    def _norm(text):
        if not text: return ""
        text = str(text).lower().strip()
        for s in [" sarl", " sas", " eurl", " sa ", " sci ", " sasu"]:
            text = text.replace(s, " ")
        text = _re.sub(r"[^a-z0-9\s]", " ", text)
        return _re.sub(r"\s+", " ", text).strip()

    def _score(lead):
        s = 0
        if lead.get("session_id") and not lead["session_id"].startswith("crm_"):
            s += 1000
        for col in ["siren", "website_url", "owner_name", "phone", "email",
                    "address", "cms", "hosting", "seo_score", "pagespeed_mobile"]:
            if lead.get(col): s += 1
        return s

    conn = sqlite3.connect(str(ROOT / config.db_path))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM leads ORDER BY id").fetchall()]

    processed, groups = set(), []
    for i, a in enumerate(rows):
        if a["id"] in processed: continue
        na = _norm(a["company_name"])
        ca = _norm(a["city"] or "")
        sa = _re.sub(r"\D", "", str(a.get("siren") or ""))
        group = [a]
        for j, b in enumerate(rows):
            if i == j or b["id"] in processed or b["id"] in {x["id"] for x in group}:
                continue
            nb = _norm(b["company_name"])
            cb = _norm(b["city"] or "")
            sb = _re.sub(r"\D", "", str(b.get("siren") or ""))
            if sa and len(sa) == 9 and sa == sb:
                group.append(b); continue
            if ca and cb and ca != cb: continue
            if SequenceMatcher(None, na, nb).ratio() >= 0.85:
                group.append(b)
        if len(group) > 1:
            for x in group:
                processed.add(x["id"])
            groups.append(group)

    ids_to_delete = []
    for group in groups:
        sorted_group = sorted(group, key=_score, reverse=True)
        ids_to_delete.extend(x["id"] for x in sorted_group[1:])

    if ids_to_delete:
        conn.executemany("DELETE FROM leads WHERE id=?", [(i,) for i in ids_to_delete])
        conn.commit()
    conn.close()
    return len(groups), len(ids_to_delete)


def _count_db_duplicates() -> tuple[int, int]:
    """Compte les doublons sans rien supprimer. Retourne (nb_groupes, nb_en_trop)."""
    import re as _re
    from difflib import SequenceMatcher

    def _norm(text):
        if not text: return ""
        text = str(text).lower().strip()
        for s in [" sarl", " sas", " eurl", " sa ", " sci ", " sasu"]:
            text = text.replace(s, " ")
        text = _re.sub(r"[^a-z0-9\s]", " ", text)
        return _re.sub(r"\s+", " ", text).strip()

    conn = sqlite3.connect(str(ROOT / config.db_path))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT id, company_name, city, siren, session_id FROM leads ORDER BY id").fetchall()]
    conn.close()

    processed, nb_groupes, nb_en_trop = set(), 0, 0
    for i, a in enumerate(rows):
        if a["id"] in processed: continue
        na = _norm(a["company_name"])
        ca = _norm(a["city"] or "")
        sa = _re.sub(r"\D", "", str(a.get("siren") or ""))
        group = [a]
        for j, b in enumerate(rows):
            if i == j or b["id"] in processed or b["id"] in {x["id"] for x in group}:
                continue
            nb = _norm(b["company_name"])
            cb = _norm(b["city"] or "")
            sb = _re.sub(r"\D", "", str(b.get("siren") or ""))
            if sa and len(sa) == 9 and sa == sb:
                group.append(b); continue
            if ca and cb and ca != cb: continue
            if SequenceMatcher(None, na, nb).ratio() >= 0.85:
                group.append(b)
        if len(group) > 1:
            for x in group: processed.add(x["id"])
            nb_groupes += 1
            nb_en_trop += len(group) - 1
    return nb_groupes, nb_en_trop


def _import_crm_file_to_db(filepath: Path) -> int:
    """Importe les entreprises d'un fichier CRM dans leads.db — doublons exclus automatiquement."""
    from core.models import Lead, LeadStatus
    from core.crm_filter import load_crm, is_in_crm

    def _int(v):
        try: return int(float(v)) if v is not None else None
        except Exception:
            log.warning("Conversion int echouee pour valeur '%s'", v, exc_info=True)
            return None

    def _float(v):
        try: return float(v) if v is not None else None
        except Exception:
            log.warning("Conversion float echouee pour valeur '%s'", v, exc_info=True)
            return None

    rows = parse_crm_file(filepath)
    if not rows:
        return 0

    # CRM existant, sans le fichier en cours d'import (pour éviter les faux positifs)
    existing_crm = [e for e in load_crm() if e["source_file"] != filepath.name]

    q = LeadQueue(
        db_path=str(ROOT / config.db_path),
        session_id=f"crm_{filepath.stem}",
        session_label=f"CRM — {filepath.name}",
    )
    imported = 0
    for r in rows:
        if existing_crm and is_in_crm(r["name"], r["city"], r["siren"] or "", existing_crm):
            continue
        q.save(Lead(
            company_name=r["name"],
            city=r["city"],
            sector=r["sector"] or "",
            source="crm_import",
            siren=r["siren"] or None,
            website_url=r["website"],
            owner_name=r["owner"],
            owner_role=r["role"],
            phone=r["phone"],
            email=r["email"],
            address=r["address"],
            google_rating=_float(r["rating"]),
            review_count=_int(r["reviews"]),
            legal_form=r["legal"],
            employee_range=r["employee"],
            cms=r["cms"],
            hosting=r["hosting"],
            pagespeed_mobile=_int(r["pagespeed"]),
            seo_score=_int(r["seo_score"]),
            seo_weaknesses=r["seo_weak"].split("|") if r["seo_weak"] else [],
            creation_date=r["creation"],
            etat=r["etat"],
            status=LeadStatus.SCRAPED,
        ))
        imported += 1
    return imported


def _import_to_analyses_db(filepath: Path) -> int:
    """Importe un fichier dans analyses_a2/analyses.db — sans toucher leads.db ni le CRM."""
    from core.models import Lead, LeadStatus

    def _int(v):
        try: return int(float(v)) if v is not None else None
        except Exception:
            log.warning("Conversion int echouee pour valeur '%s' (analyses_db)", v, exc_info=True)
            return None

    def _float(v):
        try: return float(v) if v is not None else None
        except Exception:
            log.warning("Conversion float echouee pour valeur '%s' (analyses_db)", v, exc_info=True)
            return None

    rows = parse_crm_file(filepath)
    if not rows:
        return 0
    ANALYSES_A2_DIR.mkdir(exist_ok=True)
    q = LeadQueue(
        db_path=str(ANALYSES_A2_DB),
        session_id=f"a2_{filepath.stem}",
        session_label=f"Analyse — {filepath.name}",
    )
    # WAL mode : autorise les lectures simultanées pendant que l'agent écrit
    with sqlite3.connect(str(ANALYSES_A2_DB)) as _wc:
        _wc.execute("PRAGMA journal_mode=WAL")
    for r in rows:
        q.save(Lead(
            company_name=r["name"],
            city=r["city"],
            sector=r["sector"] or "",
            source="analyse_a2",
            siren=r["siren"] or None,
            website_url=r["website"],
            owner_name=r["owner"],
            owner_role=r["role"],
            phone=r["phone"],
            email=r["email"],
            address=r["address"],
            google_rating=_float(r["rating"]),
            review_count=_int(r["reviews"]),
            legal_form=r["legal"],
            employee_range=r["employee"],
            cms=r["cms"],
            hosting=r["hosting"],
            pagespeed_mobile=_int(r["pagespeed"]),
            seo_score=_int(r["seo_score"]),
            seo_weaknesses=r["seo_weak"].split("|") if r["seo_weak"] else [],
            creation_date=r["creation"],
            etat=r["etat"],
            status=LeadStatus.SCRAPED,
        ))
    return len(rows)


# Colonnes export
COLS_A1 = {
    "company_name":"Entreprise","city":"Ville","sector":"Secteur","address":"Adresse",
    "google_rating":"Note","review_count":"Avis",
    "owner_name":"Dirigeant","owner_role":"Rôle","phone":"Téléphone",
    "website_url":"Site web","gmb_confirmed":"GMB",
    "siren":"SIREN","siret":"SIRET","tva_intra":"TVA",
    "legal_form":"Forme","etat":"État","employee_range":"Effectif",
    "naf_code":"NAF","naf_label":"Activité","creation_date":"Création",
    "siege_adresse":"Adresse siège","siege_cp":"CP","siege_ville":"Ville siège",
    "nb_etab_ouverts":"Établissements","source":"Source",
}
COLS_A2 = {
    "company_name":"Entreprise","city":"Ville","website_url":"Site web",
    "email":"Email","phone":"Téléphone",
    "cms":"CMS","hosting":"Hébergeur","agence_web":"Agence web",
    "is_https":"HTTPS","pagespeed_mobile":"Speed mobile","pagespeed_desktop":"Speed desktop",
    "copyright_year":"© Année","is_responsive":"Responsive","has_analytics":"Analytics",
    "has_meta_desc":"Meta desc","h1_count":"Nb H1",
    "has_seo_keywords":"Keywords SEO","seo_signals":"Signaux SEO",
    "seo_score":"Score SEO /10","seo_weaknesses":"Faiblesses détectées",
}
COLS_VIEW = {
    "company_name":"Entreprise","city":"Ville","sector":"Secteur",
    "google_rating":"Note","owner_name":"Dirigeant","phone":"Téléphone",
    "email":"Email","website_url":"Site","cms":"CMS","hosting":"Hébergeur",
    "pagespeed_mobile":"Speed mob","has_seo_keywords":"SEO ok",
    "seo_weaknesses":"Faiblesses","employee_range":"Effectif",
    "legal_form":"Forme","siren":"SIREN","etat":"État",
}
COLS_VENDEUR = {
    "company_name":   "Entreprise",
    "city":           "Ville",
    "sector":         "Secteur",
    "owner_name":     "Dirigeant",
    "owner_role":     "Rôle",
    "phone":          "Téléphone",
    "email":          "Email",
    "website_url":    "Site web",
    "google_rating":  "Note Google",
    "review_count":   "Avis",
    "legal_form":     "Forme juridique",
    "employee_range": "Effectif",
    "cms":            "CMS",
    "hosting":        "Hébergeur",
    "pagespeed_mobile": "Vitesse mobile",
    "seo_score":      "Score SEO /10",
    "seo_weaknesses": "Faiblesses SEO",
    "creation_date":  "Création",
    "etat":           "État",
    "siren":          "SIREN",
    "address":        "Adresse",
}


# ── Header ────────────────────────────────────────────────────────────────────
stats = db_stats()
st.markdown("<div style='padding:22px 40px 0'>", unsafe_allow_html=True)
c_logo, c_stats = st.columns([1, 3])
with c_logo:
    st.markdown(
        '<div style="font-family:Epilogue,sans-serif;font-size:18px;font-weight:900;color:#F0EBE3;letter-spacing:-0.5px">'
        '◈ leads<span style="color:#E87B2A">.</span>engine</div>'
        '<div style="font-size:10px;color:#4A4D58;letter-spacing:2px;text-transform:uppercase;margin-top:3px">'
        'Prospection automatisée</div>',
        unsafe_allow_html=True
    )
with c_stats:
    if stats:
        cx1, cx2, cx3, cx4 = st.columns(4)
        cx1.metric("Leads",           stats.get("total", 0))
        cx2.metric("Sessions",        stats.get("sessions", 0))
        cx3.metric("Analysés A2",     stats.get("analyses", 0))
        cx4.metric("Leads qualifiés", stats.get("qualifies", 0))
st.markdown("</div>", unsafe_allow_html=True)
st.markdown("<div style='height:1px;background:#1E2028;margin:16px 0 0'></div>", unsafe_allow_html=True)


# ── Bannière setup si configuration incomplète ────────────────────────────────
if not config.serpapi_key:
    st.warning("⚙️ Configuration incomplète — rends-toi dans l'onglet **Configuration** pour entrer ta clé SERPAPI_KEY avant de scraper.", icon=None)

# ── Auto-update ───────────────────────────────────────────────────────────────
if "update_checked" not in st.session_state:
    try:
        from core.updater import check_update, get_local_version
        st.session_state["update_info"] = check_update(ROOT)
        st.session_state["app_version"] = get_local_version(ROOT)
    except Exception:
        st.session_state["update_info"] = None
        st.session_state["app_version"] = "?"
    st.session_state["update_checked"] = True

_upd = st.session_state.get("update_info")
if _upd:
    _size_mb = _upd.get("size", 0) / (1024 * 1024)
    st.markdown(
        f'<div style="background:#1A1200;border:1px solid #3A2E00;border-left:3px solid #E8C32A;'
        f'border-radius:8px;padding:12px 18px;margin-bottom:12px;display:flex;'
        f'align-items:center;justify-content:space-between">'
        f'<span style="color:#F5D87A;font-size:13px;font-weight:600">'
        f'Nouvelle version {_upd["version"]} disponible ({_size_mb:.0f} Mo)</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.button("Mettre à jour", type="primary", key="btn_update"):
        from core.updater import download_and_install
        _prog = st.progress(0, text="Téléchargement en cours…")
        _ok = download_and_install(
            ROOT, _upd,
            progress_callback=lambda p: _prog.progress(
                min(p, 1.0), text=f"Téléchargement… {int(p * 100)} %"),
        )
        if _ok:
            st.success("Mise à jour téléchargée — l'application va redémarrer…")
            import time as _t; _t.sleep(2)
            import os as _os2; _os2._exit(0)
        else:
            st.error("Erreur lors de la mise à jour. Consulte errors.log.")

# ── Navigation ────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🚀  Scraping",
    "🔍  Analyse sites",
    "📋  Mes recherches",
    "🗂  CRM",
    "🌐  Tous les leads",
    "⚙️  Configuration",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Scraping
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.title("Scraping")
    st.markdown('<p style="color:#4A4D58;font-size:13px;margin-bottom:24px">Collecte les entreprises, dirigeants et données du registre national.</p>', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 2], gap="large")
    with col1:
        st.markdown('<div class="section-lbl">Cible</div>', unsafe_allow_html=True)
        secteurs    = st.text_input("Secteurs", value="plombier,électricien", key="sec1")
        ville       = st.text_input("Ville(s)", value="Aix-en-Provence", key="vil1")
        nom_session = st.text_input("Nom de la recherche", placeholder="Plombiers Aix Juin 2025", key="nom1")
    with col2:
        st.markdown('<div class="section-lbl">Paramètres</div>', unsafe_allow_html=True)
        max_r    = st.slider("Max leads / requête", 5, 100, 20, 5)
        use_maps     = st.toggle("Google Maps", value=True)
        use_registre = st.toggle("Registre National", value=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    if st.button("Lancer le scraping →", type="primary", use_container_width=True, key="btn_scrap"):
        if not secteurs.strip() or not ville.strip():
            st.error("Remplis au moins un secteur et une ville.")
        elif use_maps and not config.serpapi_key:
            st.error("SERPAPI_KEY manquante dans ton fichier .env")
        else:
            label      = nom_session.strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
            session_id = re.sub(r"[^\w\-]", "_", label)
            queries    = [(s.strip(), v.strip()) for s in secteurs.split(",") for v in ville.split(",")]
            prog = st.progress(10, text="Connexion...")
            rh, eh = [], []

            agents_ref = []

            def _sc():
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    q = LeadQueue(str(ROOT / config.db_path), session_id=session_id, session_label=label)
                    a = ScraperAgent(q)
                    agents_ref.append(a)
                    rh.append(loop.run_until_complete(a.run(
                        queries=queries, use_maps=use_maps,
                        use_registre=use_registre, max_per_query=max_r
                    )))
                    loop.close()
                except Exception as e:
                    log.error("Thread scraping (Agent 1) erreur", exc_info=True)
                    eh.append(e)

            t = threading.Thread(target=_sc, daemon=True)
            t.start()
            prog.progress(40, text="Scraping en cours...")
            _t0 = time.time()
            _chron1 = st.empty()
            while t.is_alive():
                time.sleep(1)
                _e = int(time.time() - _t0)
                _mm, _ss = divmod(_e, 60)
                _chron1.markdown(
                    f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#4A4D58">⏱ {_mm:02d}:{_ss:02d}</span>',
                    unsafe_allow_html=True
                )
            t.join(timeout=600)
            prog.progress(100, text="Terminé ✓")
            _ef = int(time.time() - _t0)
            _mmf, _ssf = divmod(_ef, 60)
            _chron1.markdown(
                f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#E87B2A">⏱ terminé en {_mmf:02d}:{_ssf:02d}</span>',
                unsafe_allow_html=True
            )

            if eh:
                st.error(f"Erreur : {eh[0]}")
            elif rh:
                leads = rh[0]
                wo = sum(1 for l in leads if l.owner_name)
                ws = sum(1 for l in leads if l.website_url)
                st.success(f"{len(leads)} leads collectés — session « {label} »")
                cx1, cx2, cx3 = st.columns(3)
                cx1.metric("Total", len(leads))
                cx2.metric("Avec site", ws)
                cx3.metric("Avec dirigeant", wo)

                # Afficher les villes explorées si extension automatique
                if agents_ref and getattr(agents_ref[0], "expanded_cities", None):
                    for query_key, cities_list in agents_ref[0].expanded_cities.items():
                        ville_origin = cities_list[0]
                        villes_ext = cities_list[1:]
                        st.info(
                            f"🗺️ **Extension automatique** pour *{query_key}* :\n"
                            f"Ville demandée **{ville_origin}** saturée → "
                            f"recherche étendue à **{len(villes_ext)}** ville(s) voisine(s) :\n"
                            f"{', '.join(villes_ext)}"
                        )

                st.info("→ Lance l'**Analyse sites** pour obtenir CMS, hébergeur, vitesse et SEO keywords.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Agent 2
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.title("Analyse des sites web")
    st.markdown('<p style="color:#4A4D58;font-size:13px;margin-bottom:24px">Extrait CMS, hébergeur, vitesse, email, agence et signaux SEO. 100% gratuit.</p>', unsafe_allow_html=True)

    db = ROOT / config.db_path
    if not db.exists():
        st.info("Lance d'abord un scraping.")
    else:
        queue2   = LeadQueue(str(ROOT / config.db_path))
        sessions = queue2.list_sessions()
        if not sessions:
            st.info("Aucune session trouvée.")
        else:
            session_map2 = {fmt_session(s): s["session_id"] for s in sessions}
            choix2   = st.selectbox("Session à analyser", list(session_map2.keys()), key="sess2")
            sid2     = session_map2[choix2]
            s2       = next(x for x in sessions if x["session_id"] == sid2)

            conn = sqlite3.connect(db)
            n_site = conn.execute("SELECT COUNT(*) FROM leads WHERE session_id=? AND website_url IS NOT NULL", (sid2,)).fetchone()[0]
            n_done = conn.execute("SELECT COUNT(*) FROM leads WHERE session_id=? AND status='extracted'", (sid2,)).fetchone()[0]
            conn.close()

            st.divider()
            cx1, cx2, cx3, cx4 = st.columns(4)
            cx1.metric("Total",     s2["total"])
            cx2.metric("Avec site", n_site)
            cx3.metric("Analysés",  n_done)
            cx4.metric("Restants",  max(0, n_site - n_done))

            st.divider()
            col1, col2 = st.columns([2, 1])
            with col1:
                delay    = st.slider("Délai entre visites (sec)", 1.0, 6.0, 2.0, 0.5)
                only_new = st.checkbox("Seulement les non analysés", value=True)
            with col2:
                reste = max(0, n_site - n_done) if only_new else n_site
                st.markdown(
                    f'<div style="margin-top:24px;background:#13151A;border:1px solid #1E2028;border-radius:8px;padding:14px 18px">'
                    f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;color:#4A4D58;margin-bottom:6px">Temps estimé</div>'
                    f'<div style="font-family:IBM Plex Mono,monospace;font-size:22px;color:#E87B2A">~{reste*(delay+9)/60:.0f} min</div>'
                    f'<div style="font-size:11px;color:#4A4D58;margin-top:2px">{reste} sites</div></div>',
                    unsafe_allow_html=True
                )

            if st.button("Lancer l'analyse →", type="primary", use_container_width=True, key="btn_a2"):
                conn = sqlite3.connect(db)
                conn.row_factory = sqlite3.Row
                if only_new:
                    sql = "SELECT * FROM leads WHERE session_id=? AND website_url IS NOT NULL AND status='scraped'"
                    rows = conn.execute(sql, (sid2,)).fetchall()
                else:
                    # Relance complète : remettre tous les leads avec site en 'scraped'
                    sql_all = "SELECT * FROM leads WHERE session_id=? AND website_url IS NOT NULL"
                    rows = conn.execute(sql_all, (sid2,)).fetchall()
                    ids_reset = [r["id"] for r in rows]
                    if ids_reset:
                        conn.executemany("UPDATE leads SET status='scraped' WHERE id=?", [(i,) for i in ids_reset])
                        conn.commit()
                        rows = conn.execute(sql_all, (sid2,)).fetchall()
                conn.close()
                leads_to_do = [dict(r) for r in rows]

                if not leads_to_do:
                    if n_site == 0:
                        st.warning("Cette session ne contient aucun lead avec un site web — rien à analyser.")
                    else:
                        st.warning("Aucun lead à analyser.")
                else:
                    prog2 = st.progress(0, text="Démarrage...")
                    rh2, eh2 = [], []

                    def _a2():
                        try:
                            rh2.append(ExtractorAgent(queue2).run(leads_to_do, delay=delay))
                        except Exception as e:
                            log.error("Thread Agent 2 erreur", exc_info=True)
                            eh2.append(e)

                    t2 = threading.Thread(target=_a2, daemon=True)
                    t2.start()
                    total2 = len(leads_to_do)
                    _t0_a2 = time.time()
                    _chron2 = st.empty()
                    while t2.is_alive():
                        time.sleep(2)
                        _e2 = int(time.time() - _t0_a2)
                        _mm2, _ss2 = divmod(_e2, 60)
                        _chron2.markdown(
                            f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#4A4D58">⏱ {_mm2:02d}:{_ss2:02d}</span>',
                            unsafe_allow_html=True
                        )
                        try:
                            c3 = sqlite3.connect(db)
                            done2 = c3.execute(
                                "SELECT COUNT(*) FROM leads WHERE session_id=? AND status IN ('extracted','skipped','error')",
                                (sid2,)
                            ).fetchone()[0] - n_done
                            c3.close()
                            pct = min(int(done2 / total2 * 100), 99) if total2 else 0
                            prog2.progress(pct, text=f"{done2}/{total2} sites traités...")
                        except Exception:
                            log.warning("Polling progression Agent 2 echoue", exc_info=True)
                    t2.join(timeout=7200)
                    prog2.progress(100, text="Terminé ✓")
                    _ef2 = int(time.time() - _t0_a2)
                    _mmf2, _ssf2 = divmod(_ef2, 60)
                    _chron2.markdown(
                        f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#E87B2A">⏱ terminé en {_mmf2:02d}:{_ssf2:02d}</span>',
                        unsafe_allow_html=True
                    )

                    if eh2:
                        st.error(f"Erreur : {eh2[0]}")
                    elif rh2:
                        r2 = rh2[0]
                        st.success(f"Analyse terminée — {r2['success']} analysés · {r2['skipped']} ignorés · {r2['errors']} erreurs")
                        st.info("→ Va dans **Mes recherches** pour voir et exporter les résultats.")

    # ── Analyser un fichier CRM ──────────────────────────────────────────────
    st.divider()
    with st.expander("◈  Analyser un fichier CRM"):
        st.markdown(
            '<p style="color:#4A4D58;font-size:13px;margin-bottom:4px">Importe un export client (Excel / CSV) et lance l\'Agent 2 dessus.</p>'
            '<p style="color:#4A4D58;font-size:12px;margin-bottom:16px">Les résultats sont stockés dans <code>analyses_a2/</code> — sans toucher à leads.db ni au CRM.</p>',
            unsafe_allow_html=True
        )

        try:
            crm_files = sorted(list(CRM_DIR.glob("*.xlsx")) + list(CRM_DIR.glob("*.csv"))) if CRM_DIR.exists() else []
        except Exception:
            log.warning("Listing fichiers CRM echoue", exc_info=True)
            crm_files = []

        src = st.radio("Source", ["Fichier déjà dans le CRM", "Importer un nouveau fichier"], horizontal=True, key="crm_a2_src")

        pending_path = None
        if src == "Fichier déjà dans le CRM":
            if not crm_files:
                st.info("Aucun fichier CRM — ajoutes-en dans l'onglet CRM.")
            else:
                file_map_a2 = {f.name: f for f in crm_files}
                chosen = st.selectbox("Fichier", list(file_map_a2.keys()), key="crm_a2_file")
                pending_path = str(file_map_a2[chosen])
        else:
            up = st.file_uploader("Fichier à analyser", type=["xlsx", "csv"], key="crm_a2_up", label_visibility="collapsed")
            if up:
                ANALYSES_A2_DIR.mkdir(exist_ok=True)
                dest_up = ANALYSES_A2_DIR / up.name
                dest_up.write_bytes(up.read())
                pending_path = str(dest_up)

        # Bouton de chargement explicite — rien ne s'exécute automatiquement
        if pending_path and st.button("Charger ce fichier →", key="btn_crm_load", type="secondary"):
            st.session_state["crm_a2_loaded"] = pending_path

        sid_crm = None
        if "crm_a2_loaded" in st.session_state:
            loaded_path = Path(st.session_state["crm_a2_loaded"])
            if loaded_path.exists():
                sid_crm = f"a2_{loaded_path.stem}"
                _import_key = f"_a2_imported_{sid_crm}"
                if not st.session_state.get(_import_key):
                    try:
                        _import_to_analyses_db(loaded_path)
                        st.session_state[_import_key] = True
                    except Exception as e:
                        log.error("Import fichier CRM dans analyses echoue", exc_info=True)
                        st.error(f"Erreur import : {e}")
                        sid_crm = None

        if sid_crm:
            queue_crm = LeadQueue(str(ANALYSES_A2_DB), session_id=sid_crm)
            conn_c = sqlite3.connect(ANALYSES_A2_DB)
            n_total_c = conn_c.execute("SELECT COUNT(*) FROM leads WHERE session_id=?", (sid_crm,)).fetchone()[0]
            n_site_c  = conn_c.execute("SELECT COUNT(*) FROM leads WHERE session_id=? AND website_url IS NOT NULL", (sid_crm,)).fetchone()[0]
            n_done_c  = conn_c.execute("SELECT COUNT(*) FROM leads WHERE session_id=? AND status IN ('extracted','skipped','error')", (sid_crm,)).fetchone()[0]
            conn_c.close()

            st.divider()
            cc1, cc2, cc3, cc4 = st.columns(4)
            cc1.metric("Total",        n_total_c)
            cc2.metric("Avec site",    n_site_c)
            cc3.metric("Analysés",     n_done_c)
            cc4.metric("Restants",     max(0, n_site_c - n_done_c))

            # ── Enrichissement sites web ─────────────────────────────────────
            n_sans_site = n_total_c - n_site_c
            if n_sans_site > 0:
                st.divider()
                enr_col1, enr_col2 = st.columns([3, 1])
                with enr_col1:
                    st.caption(f"**{n_sans_site} entreprise(s) sans site web** — recherche automatique via Google Maps")
                with enr_col2:
                    if not config.serpapi_key:
                        st.warning("Clé SerpAPI manquante", icon=None)
                    elif st.button("Trouver les sites →", key="btn_enrich_web", type="secondary", use_container_width=True):
                        delay_enr = 1.5
                        enr_rh, enr_eh = [], []
                        enr_thread = threading.Thread(
                            target=_enrich_websites,
                            args=(sid_crm, str(ANALYSES_A2_DB), delay_enr, enr_rh, enr_eh),
                            daemon=True
                        )
                        enr_thread.start()
                        enr_prog = st.progress(0, text="Recherche en cours…")
                        while enr_thread.is_alive():
                            time.sleep(2)
                            try:
                                _nc = sqlite3.connect(str(ANALYSES_A2_DB))
                                _found_so_far = _nc.execute(
                                    "SELECT COUNT(*) FROM leads WHERE session_id=? AND website_url IS NOT NULL",
                                    (sid_crm,)
                                ).fetchone()[0] - n_site_c
                                _nc.close()
                                pct = min(int(_found_so_far / n_sans_site * 100), 99) if n_sans_site else 0
                                enr_prog.progress(pct, text=f"{_found_so_far}/{n_sans_site} sites trouvés…")
                            except Exception:
                                log.warning("Polling enrichissement sites echoue", exc_info=True)
                        enr_thread.join(timeout=3600)
                        enr_prog.progress(100, text="Terminé ✓")
                        if enr_eh:
                            st.error(f"Erreur : {enr_eh[0]}")
                        elif enr_rh:
                            found_total, searched_total = enr_rh[0]
                            st.success(f"{found_total} site(s) trouvé(s) sur {searched_total} entreprise(s) recherchées.")
                        st.rerun()

            col_ca, col_cb = st.columns([2, 1])
            with col_ca:
                delay_c    = st.slider("Délai entre visites (sec)", 1.0, 6.0, 2.0, 0.5, key="delay_crm")
                only_new_c = st.checkbox("Seulement les non analysés", value=True, key="only_crm")
            with col_cb:
                reste_c = max(0, n_site_c - n_done_c) if only_new_c else n_site_c
                st.markdown(
                    f'<div style="margin-top:24px;background:#13151A;border:1px solid #1E2028;border-radius:8px;padding:14px 18px">'
                    f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;color:#4A4D58;margin-bottom:6px">Temps estimé</div>'
                    f'<div style="font-family:IBM Plex Mono,monospace;font-size:22px;color:#E87B2A">~{reste_c*(delay_c+9)/60:.0f} min</div>'
                    f'<div style="font-size:11px;color:#4A4D58;margin-top:2px">{reste_c} sites</div></div>',
                    unsafe_allow_html=True
                )

            if st.button("Analyser →", type="primary", use_container_width=True, key="btn_a2_crm"):
                conn_c2 = sqlite3.connect(ANALYSES_A2_DB)
                conn_c2.row_factory = sqlite3.Row
                sql_c = ("SELECT * FROM leads WHERE session_id=? AND website_url IS NOT NULL AND status='scraped'"
                         if only_new_c else "SELECT * FROM leads WHERE session_id=? AND website_url IS NOT NULL")
                rows_c = conn_c2.execute(sql_c, (sid_crm,)).fetchall()
                # Si relance complète, remettre tous ces leads en 'scraped' pour un suivi correct
                if not only_new_c:
                    ids_to_reset = [r["id"] for r in rows_c]
                    if ids_to_reset:
                        conn_c2.executemany(
                            "UPDATE leads SET status='scraped' WHERE id=?",
                            [(i,) for i in ids_to_reset]
                        )
                        conn_c2.commit()
                        # Recharger avec le statut réinitialisé
                        rows_c = conn_c2.execute(sql_c, (sid_crm,)).fetchall()
                conn_c2.close()
                leads_c = [dict(r) for r in rows_c]

                if not leads_c:
                    if n_site_c == 0:
                        st.warning("Ce fichier ne contient aucun site web reconnu — rien à analyser.")
                    else:
                        st.warning("Aucun lead à analyser.")
                else:
                    prog_c = st.progress(0, text="Démarrage...")
                    rh_c, eh_c = [], []

                    def _a2_crm():
                        try:
                            rh_c.append(ExtractorAgent(queue_crm).run(leads_c, delay=delay_c))
                        except Exception as e:
                            log.error("Thread Agent 2 CRM erreur", exc_info=True)
                            eh_c.append(e)

                    tc = threading.Thread(target=_a2_crm, daemon=True)
                    tc.start()
                    total_c = len(leads_c)
                    _t0_ac = time.time()
                    _chron_c = st.empty()
                    while tc.is_alive():
                        time.sleep(2)
                        _ec = int(time.time() - _t0_ac)
                        _mmc, _ssc = divmod(_ec, 60)
                        _chron_c.markdown(
                            f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#4A4D58">⏱ {_mmc:02d}:{_ssc:02d}</span>',
                            unsafe_allow_html=True
                        )
                        try:
                            cc = sqlite3.connect(ANALYSES_A2_DB)
                            done_c = cc.execute(
                                "SELECT COUNT(*) FROM leads WHERE session_id=? AND status IN ('extracted','skipped','error')",
                                (sid_crm,)
                            ).fetchone()[0] - n_done_c
                            cc.close()
                            pct_c = min(int(done_c / total_c * 100), 99) if total_c else 0
                            prog_c.progress(pct_c, text=f"{done_c}/{total_c} sites traités...")
                        except Exception:
                            log.warning("Polling progression Agent 2 CRM echoue", exc_info=True)
                    tc.join(timeout=7200)
                    prog_c.progress(100, text="Terminé ✓")
                    _efc = int(time.time() - _t0_ac)
                    _mmfc, _ssfc = divmod(_efc, 60)
                    _chron_c.markdown(
                        f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#E87B2A">⏱ terminé en {_mmfc:02d}:{_ssfc:02d}</span>',
                        unsafe_allow_html=True
                    )

                    if eh_c:
                        st.error(f"Erreur : {eh_c[0]}")
                    elif rh_c:
                        rc = rh_c[0]
                        st.success(f"Analyse terminée — {rc['success']} analysés · {rc['skipped']} ignorés · {rc['errors']} erreurs")
                        # Export automatique dans analyses_a2/
                        try:
                            conn_ex = sqlite3.connect(ANALYSES_A2_DB)
                            df_ex = pd.read_sql(
                                "SELECT * FROM leads WHERE session_id=?", conn_ex, params=(sid_crm,)
                            )
                            conn_ex.close()
                            xl_ex = to_excel_combined(df_ex, COLS_A1, COLS_A2)
                            fname_ex = f"{loaded_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                            out_path = ANALYSES_A2_DIR / fname_ex
                            out_path.write_bytes(xl_ex)
                            st.download_button(
                                f"Télécharger les résultats ({len(df_ex)} leads)",
                                data=xl_ex, file_name=fname_ex,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True, type="primary",
                            )
                            st.caption(f"Enregistré dans analyses_a2/{fname_ex}")
                        except Exception as e:
                            log.error("Export Excel apres analyse CRM echoue", exc_info=True)
                            st.error(f"Erreur export : {e}")



# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Mes recherches
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.title("Mes recherches")
    st.markdown('<p style="color:#4A4D58;font-size:13px;margin-bottom:24px">Chaque session est isolée. Filtre, explore et exporte.</p>', unsafe_allow_html=True)

    db = ROOT / config.db_path
    if not db.exists():
        st.info("Aucune recherche effectuée.")
    else:
        queue3   = LeadQueue(str(ROOT / config.db_path))
        sessions = queue3.list_sessions()
        if not sessions:
            st.info("Aucune session.")
        else:
            vue_globale = st.toggle("Toute la base", key="vue_globale3", help="Affiche tous les leads toutes sessions confondues")

            if vue_globale:
                df   = load_df()
                sid3 = "base_complete"
            else:
                session_map3 = {fmt_session(s): s["session_id"] for s in sessions}
                choix3   = st.selectbox("Session", list(session_map3.keys()), key="sess3")
                sid3     = session_map3[choix3]
                df = load_df(sid3)

            a2_ok    = has_agent2(df)
            nb_qual  = int(df.apply(is_qualifie, axis=1).sum()) if not df.empty else 0
            nb_email = int(df["email"].notna().sum())  if "email" in df.columns else 0
            nb_cms   = int(df["cms"].notna().sum())    if "cms"   in df.columns else 0
            nb_seo   = int(df["has_seo_keywords"].notna().sum()) if "has_seo_keywords" in df.columns else 0

            st.divider()
            cx1, cx2, cx3, cx4, cx5, cx6 = st.columns(6)
            cx1.metric("Total",           len(df))
            cx2.metric("Avec site",       int(df["website_url"].notna().sum()) if "website_url" in df.columns else 0)
            cx3.metric("Dirigeant",       int(df["owner_name"].notna().sum())  if "owner_name"  in df.columns else 0)
            cx4.metric("Email trouvé",    nb_email)
            cx5.metric("CMS détecté",     nb_cms)
            cx6.metric("Leads qualifiés", nb_qual)

            if a2_ok:
                st.markdown(
                    f'<div class="a2-banner">'
                    f'<div class="t">◈ Données Agent 2 disponibles</div>'
                    f'<div class="d">{nb_cms} CMS · {nb_seo} scores SEO · {nb_email} emails</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

            st.divider()

            if not df.empty:
                search = st.text_input("🔍  Rechercher par nom", placeholder="ex: Dupont, Boulangerie…", key="search3", label_visibility="collapsed")

                col_f1, col_f2, col_f3, col_f4 = st.columns(4)
                with col_f1:
                    f_qual = st.toggle(
                        "Leads qualifiés uniquement", key="fq3",
                        help="Nom + adresse + tél + ville + secteur + dirigeant + CMS + hébergeur + vitesse + SEO"
                    )
                with col_f2:
                    f_email = st.checkbox("Avec email", key="fe3")
                with col_f3:
                    f_site  = st.checkbox("Avec site web", key="fs3")
                with col_f4:
                    f_a2v = st.checkbox("Agent 2 analysé", key="fa3") if a2_ok else False

                df_show = df.copy()
                if search:          df_show = df_show[df_show["company_name"].str.contains(search, case=False, na=False)]
                if f_qual:          df_show = df_show[df_show.apply(is_qualifie, axis=1)]
                if f_email:         df_show = df_show[df_show["email"].notna()]
                if f_site:          df_show = df_show[df_show["website_url"].notna()]
                if a2_ok and f_a2v: df_show = df_show[df_show["cms"].notna()]

                st.caption(f"{len(df_show)} leads affichés")
                cols_view = [c for c in COLS_VIEW if c in df_show.columns]
                st.dataframe(
                    df_show[cols_view].rename(columns=COLS_VIEW),
                    use_container_width=True, hide_index=True, height=400
                )

                # Export
                st.markdown('<div class="export-box"><div class="export-lbl">Export Excel</div>', unsafe_allow_html=True)

                _nom_raw = st.text_input(
                    "Nom du fichier", placeholder=f"ex : Plombiers Marseille Juin 2025",
                    key="export_nom3", label_visibility="visible"
                )
                _nom_base = re.sub(r'[\\/*?:"<>|]', "", _nom_raw.strip()) or sid3

                df_qual_rows = df[df.apply(is_qualifie, axis=1)]

                ce1, ce2, ce3, ce4 = st.columns(4)
                with ce1:
                    # Toutes les colonnes A1+A2, vides si non renseignées (permet la réimportation pour analyse)
                    _all_cols_a1 = {**COLS_A1, **{k: v for k, v in COLS_A2.items() if k not in COLS_A1}}
                    _df_a1_full  = df.copy()
                    for _c in _all_cols_a1:
                        if _c not in _df_a1_full.columns:
                            _df_a1_full[_c] = None
                    _df_a1_full = _df_a1_full[[c for c in _all_cols_a1]].rename(columns=_all_cols_a1)
                    xl1 = to_excel_a1(_df_a1_full)
                    st.download_button(
                        "📄  Agent 1 — Registre national",
                        data=xl1, file_name=f"{_nom_base}_agent1.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True, type="primary",
                        help="Nom, dirigeant, SIREN, SIRET, TVA, forme juridique, effectif, adresse siège..."
                    )
                with ce2:
                    if a2_ok:
                        xl2 = to_excel_combined(df, COLS_A1, COLS_A2)
                        st.download_button(
                            "◈  Agent 1 + Agent 2 (tout en un)",
                            data=xl2, file_name=f"{_nom_base}_complet.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            help="Toutes les colonnes Agent 1 + Agent 2 fusionnées sur un seul onglet"
                        )
                    else:
                        st.markdown('<div style="color:#4A4D58;font-size:11px;padding-top:8px">Lance l\'Agent 2 pour débloquer</div>', unsafe_allow_html=True)
                with ce3:
                    if not df_qual_rows.empty:
                        xl3 = to_excel_qualifies(df_qual_rows, COLS_A1, COLS_A2)
                        st.download_button(
                            f"✓  Leads qualifiés ({len(df_qual_rows)})",
                            data=xl3, file_name=f"{_nom_base}_qualifies.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            help="Leads avec les 10 critères — colonnes A1 + A2 fusionnées"
                        )
                    else:
                        st.markdown('<div style="color:#4A4D58;font-size:11px;padding-top:8px">Aucun lead qualifié pour l\'instant</div>', unsafe_allow_html=True)
                with ce4:
                    xl_v = to_excel_vendeur(df_show)
                    st.download_button(
                        f"🎯  Vue vendeur ({len(df_show)})",
                        data=xl_v, file_name=f"{_nom_base}_vendeur.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="Colonnes essentielles pour un commercial — mise en forme incluse"
                    )

                st.divider()

                if not vue_globale:
                    with st.expander("Supprimer cette session"):
                        s3 = next(x for x in sessions if x["session_id"] == sid3)
                        st.warning(f"Supprimer définitivement « {s3.get('session_label', sid3)} » ({s3['total']} leads) ?")
                        if st.button("Confirmer la suppression", type="secondary", key="del3"):
                            queue3.delete_session(sid3)
                            st.success("Session supprimée.")
                            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CRM
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    try:
        stats_crm = crm_stats()

        st.title("Fichiers CRM")
        st.caption("Les entreprises présentes dans ces fichiers sont exclues des prochains scrapings.")

        m1, m2 = st.columns(2)
        m1.metric("Fichiers chargés",    stats_crm["fichiers"])
        m2.metric("Entreprises connues", stats_crm["entreprises"])

        st.divider()
        st.subheader("Ajouter un fichier")
        uploaded = st.file_uploader(
            "Ajouter", type=["xlsx", "csv"],
            accept_multiple_files=True, key="crm_upload",
            label_visibility="collapsed",
        )
        if uploaded:
            CRM_DIR.mkdir(exist_ok=True)
            n_imp = 0
            n_total = 0
            for f in uploaded:
                dest = CRM_DIR / f.name
                raw = f.read()
                # Compter le total avant import pour calculer les doublons ignorés
                try:
                    import io as _io
                    _df_count = pd.read_excel(_io.BytesIO(raw)) if f.name.endswith(".xlsx") else pd.read_csv(_io.BytesIO(raw))
                    n_total += len(_df_count)
                except Exception:
                    log.warning("Comptage lignes CRM echoue pour '%s'", f.name, exc_info=True)
                dest.write_bytes(raw)
                n_imp += _import_crm_file_to_db(dest)
            n_skip = n_total - n_imp
            msg = f"{len(uploaded)} fichier(s) ajouté(s) · {n_imp} entreprise(s) intégrée(s)"
            if n_skip > 0:
                msg += f" · {n_skip} doublon(s) ignoré(s)"
            st.success(msg + ".")
            st.rerun()

        st.divider()
        st.subheader("Fichiers actifs")
        if stats_crm["liste"]:
            for item in stats_crm["liste"]:
                c_n, c_nb, c_s, c_d = st.columns([4, 1, 1, 1])
                c_n.write(f"📄 {item['nom']}")
                c_nb.caption(f"{item['nb']} lignes")
                if c_s.button("Synchroniser", key=f"sync_{item['nom']}", type="secondary"):
                    n = _import_crm_file_to_db(Path(item["path"]))
                    st.success(f"{n} entreprise(s) synchronisée(s).")
                    st.rerun()
                if c_d.button("Supprimer", key=f"del_{item['nom']}", type="secondary"):
                    Path(item["path"]).unlink(missing_ok=True)
                    st.success(f"« {item['nom']} » supprimé.")
                    st.rerun()
        else:
            st.info("Aucun fichier CRM actif.")

        st.divider()
        st.subheader("Comparer un fichier")
        st.caption("Vérifie lesquelles de ces entreprises sont déjà dans ton CRM — sans l'ajouter à la base.")
        cmp_file = st.file_uploader(
            "Comparer", type=["xlsx", "csv"],
            key="crm_compare", label_visibility="collapsed",
        )
        if cmp_file:
            res = compare_against_crm(cmp_file, cmp_file.name)
            if res.get("error"):
                st.error(res["error"])
            else:
                r1, r2, r3 = st.columns(3)
                r1.metric("Total analysé",    res["total"])
                r2.metric("Déjà dans le CRM", len(res["doublons"]))
                r3.metric("Nouveaux",          len(res["nouveaux"]))
                if res["doublons"]:
                    st.markdown("**Doublons**")
                    st.dataframe(pd.DataFrame(res["doublons"]), use_container_width=True, hide_index=True)
                if res["nouveaux"]:
                    st.markdown("**Nouveaux**")
                    st.dataframe(pd.DataFrame(res["nouveaux"]), use_container_width=True, hide_index=True)
    except Exception as e:
        log.error("Erreur onglet CRM", exc_info=True)
        st.error(f"Erreur CRM : {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Tous les leads
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    try:
        st.title("Tous les leads")
        st.caption("Vue globale toutes sessions confondues.")

        df_all = load_df()
        if df_all.empty:
            st.info("Aucun lead en base.")
        else:
            nb_qual_all  = int(df_all.apply(is_qualifie, axis=1).sum())
            nb_email_all = int(df_all["email"].notna().sum()) if "email" in df_all.columns else 0
            a2_g = has_agent2(df_all)

            t1, t2, t3, t4, t5 = st.columns(5)
            t1.metric("Total",          len(df_all))
            t2.metric("Avec site",      int(df_all["website_url"].notna().sum()))
            t3.metric("Avec dirigeant", int(df_all["owner_name"].notna().sum()))
            t4.metric("Avec email",     nb_email_all)
            t5.metric("Qualifiés",      nb_qual_all)

            st.divider()

            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                f_s = st.multiselect("Secteur", sorted(df_all["sector"].dropna().unique()), key="s4")
            with fc2:
                f_v = st.multiselect("Ville", sorted(df_all["city"].dropna().unique()), key="v4")
            with fc3:
                sess_all = LeadQueue(str(ROOT / config.db_path)).list_sessions()
                sid_map4 = {fmt_session(s): s["session_id"] for s in sess_all}
                f_se = st.multiselect("Session", list(sid_map4.keys()), key="se4")
                sids_sel = [sid_map4[l] for l in f_se]

            fc4, fc5, fc6 = st.columns(3)
            with fc4: f_q  = st.toggle("Qualifiés uniquement", key="fq4")
            with fc5: f_m  = st.checkbox("Avec email", key="fe4")
            with fc6: f_si = st.checkbox("Avec site",  key="fs4")

            df_f = df_all.copy()
            if f_s:      df_f = df_f[df_f["sector"].isin(f_s)]
            if f_v:      df_f = df_f[df_f["city"].isin(f_v)]
            if sids_sel: df_f = df_f[df_f["session_id"].isin(sids_sel)]
            if f_q:      df_f = df_f[df_f.apply(is_qualifie, axis=1)]
            if f_m:      df_f = df_f[df_f["email"].notna()]
            if f_si:     df_f = df_f[df_f["website_url"].notna()]

            st.caption(f"{len(df_f)} leads affichés")
            cols_v4 = [c for c in COLS_VIEW if c in df_f.columns]
            st.dataframe(df_f[cols_v4].rename(columns=COLS_VIEW),
                         use_container_width=True, hide_index=True, height=480)

            st.divider()
            ex1, ex2 = st.columns(2)
            with ex1:
                xl_g = to_excel_combined(df_f, COLS_A1, COLS_A2) if a2_g else to_excel_a1(df_f[[c for c in COLS_A1 if c in df_f.columns]].rename(columns=COLS_A1))
                st.download_button(f"Exporter la sélection ({len(df_f)})", data=xl_g,
                                   file_name="leads_global.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True, type="primary")
            with ex2:
                xl_gv = to_excel_vendeur(df_f)
                st.download_button(f"🎯  Vue vendeur ({len(df_f)})", data=xl_gv,
                                   file_name="leads_vendeur.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)

            st.divider()
            with st.expander("Nettoyer les doublons"):
                st.caption("Analyse toute la base et supprime les entrées en double (même entreprise dans plusieurs sessions ou fichiers CRM).")
                dc1, dc2 = st.columns([3, 1])
                with dc1:
                    if st.button("Analyser les doublons", key="btn_count_dups", type="secondary", use_container_width=True):
                        with st.spinner("Analyse en cours…"):
                            nb_grp, nb_trop = _count_db_duplicates()
                        if nb_grp == 0:
                            st.success("Aucun doublon détecté.")
                        else:
                            st.warning(f"{nb_grp} groupe(s) de doublons · {nb_trop} entrée(s) en trop.")
                            st.session_state["_dup_found"] = (nb_grp, nb_trop)
                with dc2:
                    dup_info = st.session_state.get("_dup_found")
                    btn_label = f"Supprimer ({dup_info[1]})" if dup_info else "Supprimer"
                    if st.button(btn_label, key="btn_del_dups", type="primary",
                                 use_container_width=True, disabled=not dup_info):
                        with st.spinner("Suppression…"):
                            _, nb_del = _find_db_duplicates()
                        st.session_state.pop("_dup_found", None)
                        st.success(f"{nb_del} doublon(s) supprimé(s).")
                        st.rerun()
    except Exception as e:
        log.error("Erreur onglet Tous les leads", exc_info=True)
        st.error(f"Erreur : {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — Configuration / Setup
# ══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.title("Configuration")
    st.markdown('<p style="color:#4A4D58;font-size:13px;margin-bottom:24px">Vérifie et complète la configuration nécessaire au bon fonctionnement de l\'application.</p>', unsafe_allow_html=True)

    def _save_env_keys(**kwargs):
        """Met à jour ROOT/.env en conservant les clés existantes."""
        env_path = ROOT / ".env"
        lines = []
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith("#"):
                    lines.append(line)
                    continue
                k = s.split("=", 1)[0]
                if k not in kwargs:
                    lines.append(line)
        for k, v in kwargs.items():
            if v.strip():
                lines.append(f"{k}={v.strip()}")
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        load_dotenv(env_path, override=True)
        import os as _osx
        config.serpapi_key   = _osx.getenv("SERPAPI_KEY",        "")
        config.pagespeed_key = _osx.getenv("PAGESPEED_API_KEY",  "")
        config.anthropic_key = _osx.getenv("ANTHROPIC_API_KEY",  "")

    # ── Bannière statut global ────────────────────────────────────────────────
    _pw_ok    = any((ROOT / "playwright_browsers").glob("chromium-*")) if (ROOT / "playwright_browsers").exists() else False
    _serp_ok  = bool(config.serpapi_key)
    _all_ok   = _serp_ok and _pw_ok and (ROOT / config.db_path).exists()

    if _all_ok:
        st.success("Tout est configuré — l'application est prête à l'emploi.")
    else:
        missing = []
        if not _serp_ok: missing.append("clé SERPAPI_KEY")
        if not _pw_ok:   missing.append("navigateur Chromium (analyse sites)")
        st.warning(f"Configuration incomplète : {', '.join(missing)}. Complète les sections ci-dessous.")

    st.divider()

    # ── Section 1 : Clés API ──────────────────────────────────────────────────
    st.markdown('<div class="section-lbl">Clés API</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        inp_serpapi = st.text_input(
            "SERPAPI_KEY — Google Maps ✱ obligatoire",
            value=config.serpapi_key,
            type="password",
            key="cfg_serpapi",
            help="Clé SerpAPI pour le scraping Google Maps. Obtenir sur serpapi.com."
        )
    with c2:
        inp_pagespeed = st.text_input(
            "PAGESPEED_API_KEY — Google PageSpeed (optionnel)",
            value=config.pagespeed_key,
            type="password",
            key="cfg_pagespeed",
            help="Pour les scores de vitesse mobile/desktop. Obtenir sur console.cloud.google.com."
        )
    inp_anthropic = st.text_input(
        "ANTHROPIC_API_KEY (optionnel)",
        value=config.anthropic_key,
        type="password",
        key="cfg_anthropic",
    )

    if st.button("Enregistrer les clés →", type="primary", key="btn_cfg_save"):
        _save_env_keys(
            SERPAPI_KEY=inp_serpapi,
            PAGESPEED_API_KEY=inp_pagespeed,
            ANTHROPIC_API_KEY=inp_anthropic,
        )
        st.success("Clés enregistrées et actives.")
        st.rerun()

    st.divider()

    # ── Section 2 : Fichiers & dossiers ──────────────────────────────────────
    st.markdown('<div class="section-lbl">Fichiers de données</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="font-family:IBM Plex Mono,monospace;font-size:11px;color:#4A4D58;margin-bottom:12px">Dossier racine : {ROOT}</div>', unsafe_allow_html=True)

    _items = [
        ("leads.db",       ROOT / config.db_path, False),
        ("crm/",           ROOT / "crm",           False),
        ("analyses_a2/",   ROOT / "analyses_a2",   False),
        (".env",           ROOT / ".env",           False),
        ("playwright_browsers/", ROOT / "playwright_browsers", True),
    ]
    for _lbl, _path, _warn_only in _items:
        _ok    = _path.exists()
        _color = "#2ECC71" if _ok else ("#E8C32A" if _warn_only else "#E84C4C")
        _icon  = "✓" if _ok else "⚠" if _warn_only else "✗"
        st.markdown(
            f'<div style="font-family:IBM Plex Mono,monospace;font-size:12px;margin-bottom:6px">'
            f'<span style="color:{_color};font-weight:700">{_icon}</span>&nbsp;&nbsp;'
            f'<span style="color:#C8C2BB">{_lbl}</span>&nbsp;&nbsp;'
            f'<span style="color:#4A4D58">{_path}</span></div>',
            unsafe_allow_html=True,
        )

    if st.button("Créer les dossiers manquants →", key="btn_cfg_mkdir"):
        (ROOT / "crm").mkdir(parents=True, exist_ok=True)
        (ROOT / "analyses_a2").mkdir(parents=True, exist_ok=True)
        LeadQueue(str(ROOT / config.db_path))
        st.success("Dossiers et base de données initialisés.")
        st.rerun()

    st.divider()

    # ── Section 3 : Navigateur Playwright ────────────────────────────────────
    st.markdown('<div class="section-lbl">Navigateur — Analyse sites</div>', unsafe_allow_html=True)

    if _pw_ok:
        st.success("Chromium disponible — l'analyse des sites web (Agent 2) est active.")
    else:
        st.error(
            "Chromium absent du dossier `playwright_browsers/`. "
            "L'analyse des sites web (Agent 2) est désactivée. "
            "Demande une version complète du package à l'administrateur."
        )

    st.divider()

    # ── Section 4 : Sources de données ─────────────────────────────────────
    st.markdown('<div class="section-lbl">Sources de données — Scraping</div>', unsafe_allow_html=True)
    st.markdown(
        """
        | Source | Type | Licence | Données |
        |--------|------|---------|---------|
        | **Google Maps** (SerpAPI) | API payante | Usage commercial OK | Nom, adresse, téléphone, site web, avis |
        | **Registre National** (recherche-entreprises.api.gouv.fr) | API gratuite | Licence ouverte Etalab | Nom, adresse, SIREN, NAF, dirigeants, effectifs |
        | **Dirigeants** (recherche-entreprises.api.gouv.fr) | API gratuite | Licence ouverte Etalab | Dirigeant, forme juridique, date création |
        | **Géocodage** (geo.api.gouv.fr) | API gratuite | Licence ouverte Etalab | Communes voisines, coordonnées GPS |
        | **Google PageSpeed** | API gratuite | Google ToS | Scores mobile/desktop |

        Toutes les sources utilisées sont **légales et autorisent l'usage commercial**.
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    # ── Section 5 : Fermeture application ──────────────────────────────────
    st.markdown('<div class="section-lbl">Application</div>', unsafe_allow_html=True)
    st.markdown(
        '<p style="color:#4A4D58;font-size:13px">Ferme proprement l\'application et toutes ses instances.</p>',
        unsafe_allow_html=True,
    )
    if st.button("Fermer l'application", type="secondary", use_container_width=True, key="btn_quit"):
        st.warning("Fermeture en cours...")
        import signal
        # Tuer tous les processus LeadsEngine (mode exe)
        if getattr(sys, "frozen", False):
            _os.system('taskkill /F /IM LeadsEngine.exe >nul 2>&1')
        else:
            # Mode dev : arrêter proprement Streamlit
            _os.kill(_os.getpid(), signal.SIGTERM)
