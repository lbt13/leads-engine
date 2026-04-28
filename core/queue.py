import sqlite3
from datetime import datetime
from core.models import Lead, LeadStatus
from core.logger import get_logger

log = get_logger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS leads (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        TEXT,
    session_label     TEXT,
    company_name      TEXT NOT NULL,
    city              TEXT, sector TEXT, source TEXT, address TEXT,
    google_rating     REAL, review_count INTEGER, website_url TEXT,
    gmb_confirmed     INTEGER DEFAULT 0, gmb_url TEXT, gmb_category TEXT,
    owner_name TEXT, owner_role TEXT, all_owners TEXT,
    siren TEXT, siret TEXT, tva_intra TEXT,
    legal_form TEXT, legal_form_code TEXT, etat TEXT, capital_social TEXT,
    naf_code TEXT, naf_label TEXT, section_activite TEXT,
    employee_range TEXT, employee_code TEXT, annee_effectif TEXT,
    creation_date TEXT, date_mise_a_jour TEXT,
    siege_adresse TEXT, siege_cp TEXT, siege_ville TEXT,
    nb_etablissements INTEGER, nb_etab_ouverts INTEGER,
    est_ei INTEGER, est_association INTEGER,
    phone TEXT, email TEXT, contact_name TEXT,
    cms TEXT, hosting TEXT, agence_web TEXT,
    is_https INTEGER, pagespeed_mobile INTEGER, pagespeed_desktop INTEGER,
    page_count INTEGER, page_title TEXT, has_meta_desc INTEGER,
    h1_count INTEGER, is_responsive INTEGER, has_analytics INTEGER,
    has_google_ads INTEGER, domain_age TEXT,
    copyright_year TEXT,
    has_seo_keywords INTEGER,
    seo_signals TEXT,
    seo_score INTEGER,
    seo_weaknesses TEXT, seo_summary TEXT,
    social_facebook TEXT, social_instagram TEXT,
    social_linkedin TEXT, social_twitter TEXT,
    social_youtube TEXT, social_tiktok TEXT,
    social_pinterest TEXT, social_count INTEGER,
    hook TEXT,
    score REAL, status TEXT DEFAULT 'scraped',
    error_message TEXT, scraped_at TEXT, updated_at TEXT,
    UNIQUE(session_id, company_name, city, source)
);
CREATE INDEX IF NOT EXISTS idx_status  ON leads(status);
CREATE INDEX IF NOT EXISTS idx_session ON leads(session_id);
"""

ALL_NEW_COLS = {
    "session_id":"TEXT","session_label":"TEXT",
    "gmb_confirmed":"INTEGER DEFAULT 0","gmb_url":"TEXT","gmb_category":"TEXT",
    "owner_name":"TEXT","owner_role":"TEXT","all_owners":"TEXT",
    "siren":"TEXT","siret":"TEXT","tva_intra":"TEXT",
    "legal_form":"TEXT","legal_form_code":"TEXT","etat":"TEXT","capital_social":"TEXT",
    "naf_code":"TEXT","naf_label":"TEXT","section_activite":"TEXT",
    "employee_range":"TEXT","employee_code":"TEXT","annee_effectif":"TEXT",
    "creation_date":"TEXT","date_mise_a_jour":"TEXT",
    "siege_adresse":"TEXT","siege_cp":"TEXT","siege_ville":"TEXT",
    "nb_etablissements":"INTEGER","nb_etab_ouverts":"INTEGER",
    "est_ei":"INTEGER","est_association":"INTEGER",
    "agence_web":"TEXT","is_https":"INTEGER",
    "page_title":"TEXT","has_meta_desc":"INTEGER",
    "h1_count":"INTEGER","is_responsive":"INTEGER",
    "has_analytics":"INTEGER","has_google_ads":"INTEGER","domain_age":"TEXT",
    "copyright_year":"TEXT",
    "has_seo_keywords":"INTEGER","seo_signals":"TEXT","seo_score":"INTEGER",
    "social_facebook":"TEXT","social_instagram":"TEXT",
    "social_linkedin":"TEXT","social_twitter":"TEXT",
    "social_youtube":"TEXT","social_tiktok":"TEXT",
    "social_pinterest":"TEXT","social_count":"INTEGER",
    "call_status":"TEXT DEFAULT 'non_appele'","lead_notes":"TEXT",
    "tags":"TEXT","lead_history":"TEXT",
}


class LeadQueue:
    def __init__(self, db_path="leads.db", session_id=None, session_label=None):
        self.db_path      = db_path
        self.session_id   = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_label = session_label or self.session_id
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(DDL)
        self._migrate()

    def _migrate(self):
        with self._conn() as conn:
            existing = {r[1] for r in conn.execute("PRAGMA table_info(leads)").fetchall()}
            for col, typ in ALL_NEW_COLS.items():
                if col not in existing:
                    try:
                        conn.execute(f"ALTER TABLE leads ADD COLUMN {col} {typ}")
                        log.info("Migration : colonne '%s' ajoutée", col)
                    except Exception:
                        pass

    def save(self, lead: Lead) -> int:
        d = lead.to_dict()
        d["session_id"]    = self.session_id
        d["session_label"] = self.session_label
        d["updated_at"]    = datetime.now().isoformat()
        cols  = ", ".join(d.keys())
        ph    = ", ".join(["?"] * len(d))
        upd   = ", ".join(f"{k}=excluded.{k}" for k in d if k not in ("session_id","company_name","city","source"))
        sql   = f"INSERT INTO leads ({cols}) VALUES ({ph}) ON CONFLICT(session_id,company_name,city,source) DO UPDATE SET {upd}"
        with self._conn() as conn:
            return conn.execute(sql, list(d.values())).lastrowid

    def update_status(self, lead_id, status: LeadStatus, error=None):
        with self._conn() as conn:
            conn.execute("UPDATE leads SET status=?,error_message=?,updated_at=? WHERE id=?",
                         (status.value, error, datetime.now().isoformat(), lead_id))

    def update_fields(self, lead_id, **kwargs):
        kwargs["updated_at"] = datetime.now().isoformat()
        sets = ", ".join(f"{k}=?" for k in kwargs)
        with self._conn() as conn:
            conn.execute(f"UPDATE leads SET {sets} WHERE id=?", [*kwargs.values(), lead_id])

    def get_by_status(self, status: LeadStatus) -> list:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM leads WHERE status=? ORDER BY id", (status.value,)).fetchall()
        return [dict(r) for r in rows]

    def list_sessions(self) -> list:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT session_id, session_label,
                    COUNT(*) as total,
                    GROUP_CONCAT(DISTINCT sector) as secteurs,
                    GROUP_CONCAT(DISTINCT city)   as villes,
                    SUM(CASE WHEN website_url IS NOT NULL THEN 1 ELSE 0 END) as avec_site,
                    SUM(CASE WHEN owner_name  IS NOT NULL THEN 1 ELSE 0 END) as avec_dirigeant,
                    SUM(CASE WHEN siren       IS NOT NULL THEN 1 ELSE 0 END) as avec_siren,
                    SUM(CASE WHEN email       IS NOT NULL THEN 1 ELSE 0 END) as avec_email,
                    SUM(gmb_confirmed) as gmb,
                    MIN(scraped_at)    as date_debut
                FROM leads GROUP BY session_id ORDER BY date_debut DESC
            """).fetchall()
        return [dict(r) for r in rows]

    def get_by_session(self, session_id) -> list:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM leads WHERE session_id=? ORDER BY google_rating DESC", (session_id,)).fetchall()
        return [dict(r) for r in rows]

    def delete_session(self, session_id):
        with self._conn() as conn:
            conn.execute("DELETE FROM leads WHERE session_id=?", (session_id,))

    def get_existing_leads(self, exclude_session_id: str = None) -> list:
        """Retourne les (company_name, city) de toutes les sessions précédentes."""
        with self._conn() as conn:
            if exclude_session_id:
                rows = conn.execute(
                    "SELECT company_name, city FROM leads WHERE session_id != ?",
                    (exclude_session_id,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT company_name, city FROM leads").fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute("SELECT status, COUNT(*) as n FROM leads GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}
