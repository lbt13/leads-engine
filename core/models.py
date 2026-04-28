from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime

class LeadStatus(Enum):
    SCRAPED   = "scraped"
    EXTRACTED = "extracted"
    ANALYZED  = "analyzed"
    SCORED    = "scored"
    EXPORTED  = "exported"
    SKIPPED   = "skipped"
    ERROR     = "error"

@dataclass
class Lead:
    company_name:      str
    city:              str
    sector:            str
    source:            str
    address:           Optional[str]   = None
    google_rating:     Optional[float] = None
    review_count:      Optional[int]   = None
    website_url:       Optional[str]   = None
    gmb_confirmed:     bool            = False
    gmb_url:           Optional[str]   = None
    gmb_category:      Optional[str]   = None
    owner_name:        Optional[str]   = None
    owner_role:        Optional[str]   = None
    all_owners:        Optional[str]   = None
    siren:             Optional[str]   = None
    siret:             Optional[str]   = None
    tva_intra:         Optional[str]   = None
    legal_form:        Optional[str]   = None
    legal_form_code:   Optional[str]   = None
    etat:              Optional[str]   = None
    capital_social:    Optional[str]   = None
    naf_code:          Optional[str]   = None
    naf_label:         Optional[str]   = None
    section_activite:  Optional[str]   = None
    employee_range:    Optional[str]   = None
    employee_code:     Optional[str]   = None
    annee_effectif:    Optional[str]   = None
    creation_date:     Optional[str]   = None
    date_mise_a_jour:  Optional[str]   = None
    siege_adresse:     Optional[str]   = None
    siege_cp:          Optional[str]   = None
    siege_ville:       Optional[str]   = None
    nb_etablissements: Optional[int]   = None
    nb_etab_ouverts:   Optional[int]   = None
    est_ei:            Optional[bool]  = None
    est_association:   Optional[bool]  = None
    phone:             Optional[str]   = None
    email:             Optional[str]   = None
    contact_name:      Optional[str]   = None
    cms:               Optional[str]   = None
    hosting:           Optional[str]   = None
    agence_web:        Optional[str]   = None
    is_https:          Optional[bool]  = None
    pagespeed_mobile:  Optional[int]   = None
    pagespeed_desktop: Optional[int]   = None
    page_count:        Optional[int]   = None
    page_title:        Optional[str]   = None
    has_meta_desc:     Optional[bool]  = None
    h1_count:          Optional[int]   = None
    is_responsive:     Optional[bool]  = None
    has_analytics:     Optional[bool]  = None
    has_google_ads:    Optional[bool]  = None
    domain_age:        Optional[str]   = None
    copyright_year:    Optional[str]   = None
    has_seo_keywords:  Optional[bool]  = None
    seo_signals:       Optional[str]   = None
    seo_score:         Optional[int]   = None
    seo_weaknesses:    list            = field(default_factory=list)
    seo_summary:       Optional[str]   = None
    social_facebook:   Optional[str]   = None
    social_instagram:  Optional[str]   = None
    social_linkedin:   Optional[str]   = None
    social_twitter:    Optional[str]   = None
    social_youtube:    Optional[str]   = None
    social_tiktok:     Optional[str]   = None
    social_pinterest:  Optional[str]   = None
    social_count:      Optional[int]   = None
    hook:              Optional[str]   = None
    score:             Optional[float] = None
    status:            LeadStatus      = LeadStatus.SCRAPED
    error_message:     Optional[str]   = None
    scraped_at:        str             = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        d["status"]           = self.status.value
        d["gmb_confirmed"]    = int(self.gmb_confirmed)
        d["seo_weaknesses"]   = "|".join(self.seo_weaknesses)
        for f in ("est_ei","est_association","is_https","has_meta_desc","is_responsive","has_analytics","has_google_ads","has_seo_keywords"):
            if d.get(f) is not None: d[f] = int(d[f])
        return d
