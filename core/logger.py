"""
core/logger.py — Systeme de logging centralise pour leads_engine.

Produit un fichier errors.log structure et optimise pour l'analyse
automatique par Claude. Toutes les erreurs, meme silencieuses,
y sont consignees avec traceback complet et contexte.

Usage dans chaque module :
    from core.logger import get_logger
    log = get_logger(__name__)
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Chemins ──────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
ERRORS_LOG = _ROOT / "errors.log"
PIPELINE_LOG = _ROOT / "pipeline.log"

# ── Format optimise pour analyse Claude ──────────────────────────────────────
# Chaque entree contient : date complete, niveau, module, message
# Le traceback complet suit directement l'entree sur les lignes suivantes.
ERROR_FMT = (
    "────────────────────────────────────────────────────────────────\n"
    "%(asctime)s | %(levelname)-8s | %(name)s\n"
    "%(message)s"
)
ERROR_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# Format console + pipeline.log (compact)
CONSOLE_FMT = "%(asctime)s %(levelname)-8s %(message)s"
CONSOLE_DATE_FMT = "%H:%M:%S"

_initialized = False


def setup_logging():
    """
    Configure le logging pour toute l'application.
    Appeler UNE SEULE FOIS au demarrage (main.py ou app.py).

    Produit 3 sorties :
      1. Console   — INFO+  — format compact
      2. pipeline.log — INFO+  — historique complet des operations
      3. errors.log   — WARNING+ — uniquement les erreurs, avec traceback
    """
    global _initialized
    if _initialized:
        return
    _initialized = True

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Nettoyer les handlers existants (evite les doublons si basicConfig a deja ete appele)
    root.handlers.clear()

    # 1. Console — INFO+
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(CONSOLE_FMT, datefmt=CONSOLE_DATE_FMT))
    root.addHandler(console)

    # 2. pipeline.log — INFO+ (historique complet, rotation 5 Mo)
    pipeline = RotatingFileHandler(
        str(PIPELINE_LOG), maxBytes=5_000_000, backupCount=2, encoding="utf-8"
    )
    pipeline.setLevel(logging.INFO)
    pipeline.setFormatter(logging.Formatter(CONSOLE_FMT, datefmt=CONSOLE_DATE_FMT))
    root.addHandler(pipeline)

    # 3. errors.log — WARNING+ (erreurs uniquement, rotation 2 Mo)
    errors = RotatingFileHandler(
        str(ERRORS_LOG), maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    errors.setLevel(logging.WARNING)
    errors.setFormatter(logging.Formatter(ERROR_FMT, datefmt=ERROR_DATE_FMT))
    root.addHandler(errors)


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger nomme. Initialise le systeme au premier appel.
    Remplace logging.getLogger(__name__) dans chaque module.
    """
    setup_logging()
    return logging.getLogger(name)
