import os
from dataclasses import dataclass, field


@dataclass
class Config:
    serpapi_key:           str   = ""
    anthropic_key:         str   = ""
    pagespeed_key:         str   = ""
    max_results_per_query: int   = 60
    playwright_timeout_ms: int   = 15000
    playwright_headless:   bool  = True
    delay_between_pages_s: float = 2.0
    delay_serpapi_s:       float = 1.0
    min_google_rating:     float = 0.0
    min_review_count:      int   = 0
    skip_cms:              list  = field(default_factory=list)
    output_dir:            str   = "output"
    db_path:               str   = "leads.db"

    def __post_init__(self):
        self.serpapi_key   = os.getenv("SERPAPI_KEY", "")
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.pagespeed_key = os.getenv("PAGESPEED_API_KEY", "")


config = Config()
