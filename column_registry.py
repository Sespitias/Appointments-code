"""
Centralized column registry for the Appointments dataset.

Contains mappings, constants, and helper functions used across
the transformation pipeline. This module is the single source of truth
for business-rule catalogs and column metadata.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Week configuration
# ---------------------------------------------------------------------------
# Python weekday(): Monday=0 … Sunday=6.  Friday=4.
CUSTOM_WEEK_START_DAY = 4  # Friday

# ---------------------------------------------------------------------------
# ConfirmationStatus → Done mapping (local fallback catalog)
# ---------------------------------------------------------------------------
# Minimum guaranteed mapping. The master sheet can override or extend this.
CONFIRMATION_STATUS_DONE_MAP: dict[str, int] = {
    "Check-Out": 1,   # canonical casing (keep for safety)
    "Check-out": 1,   # actual casing returned by the Tebra API
}


def build_done_map(master_df=None) -> dict[str, int]:
    """
    Build the consolidated Done-mapping dictionary.

    Priority: master_df values override the local catalog.

    Parameters
    ----------
    master_df : pd.DataFrame | None
        DataFrame with columns ``ConfirmationStatus`` and ``Done``.

    Returns
    -------
    dict[str, int]
        Mapping from ConfirmationStatus values to 0/1.
    """
    merged = dict(CONFIRMATION_STATUS_DONE_MAP)

    if master_df is not None:
        if {"ConfirmationStatus", "Done"}.issubset(master_df.columns):
            master_map = dict(
                zip(master_df["ConfirmationStatus"], master_df["Done"])
            )
            merged.update(master_map)
        else:
            logger.warning(
                "master_df missing ConfirmationStatus/Done columns; "
                "using local catalog only"
            )

    return merged


# ---------------------------------------------------------------------------
# String columns that need trimming
# ---------------------------------------------------------------------------
TRIM_COLUMNS: list[str] = [
    "ConfirmationStatus",
    "ServiceLocationName",
    "PatientCaseName",
    "PatientCasePayerScenario",
    "AppointmentReason1",
    "Provider",
    "Service",
    "CaseNameID",
]

# ---------------------------------------------------------------------------
# Datetime columns that must be parsed early
# ---------------------------------------------------------------------------
DATETIME_COLUMNS: list[str] = [
    "StartDate",
    "EndDate",
    "CreatedDate",
    "LastModifiedDate",
]

# ---------------------------------------------------------------------------
# Schema contract (loaded lazily from Campos.json)
# ---------------------------------------------------------------------------
_SCHEMA_CACHE: dict | None = None


def load_schema(path: str | Path | None = None) -> dict:
    """Load and cache the column schema from Campos.json."""
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE

    if path is None:
        path = Path(__file__).parent / "Campos.json"
    else:
        path = Path(path)

    with open(path, "r", encoding="utf-8") as fh:
        _SCHEMA_CACHE = json.load(fh)
    return _SCHEMA_CACHE
