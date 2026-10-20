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
# AppointmentReason1 → Service mapping (local fallback catalog)
# ---------------------------------------------------------------------------
# Minimum guaranteed mapping. The master sheet can override or extend this.
APPOINTMENT_REASON_SERVICE_MAP: dict[str, str] = {
    "Mindmetrix": "Cognitive",
    "Mindmetrix Retake": "Cognitive",
}


_MISSING_TEXT = {"", "nan", "none"}


def is_missing_text(value) -> bool:
    """True for None, NaN, or text that is blank / "nan" / "none" (any case)."""
    if value is None:
        return True
    try:
        if value != value:  # NaN
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip().casefold() in _MISSING_TEXT


def normalize_reason_key(value) -> str | None:
    """Normalize an appointment reason for matching (trim + casefold).

    Returns ``None`` for ``None``, NaN, or blank values.
    """
    if value is None:
        return None
    try:
        if value != value:  # NaN
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip().casefold()
    return text or None


def build_service_map(master_df=None) -> dict[str, str]:
    """
    Build the consolidated AppointmentReason1 → Service dictionary.

    Keys are normalized with :func:`normalize_reason_key`.
    Priority: master_df values override the local catalog.

    Parameters
    ----------
    master_df : pd.DataFrame | None
        DataFrame with columns ``AppointmentReason1`` and ``Service``.
    """
    merged: dict[str, str] = {}
    for reason, service in APPOINTMENT_REASON_SERVICE_MAP.items():
        merged[normalize_reason_key(reason)] = service

    if master_df is not None:
        if {"AppointmentReason1", "Service"}.issubset(master_df.columns):
            for reason, service in zip(
                master_df["AppointmentReason1"], master_df["Service"]
            ):
                key = normalize_reason_key(reason)
                if key is not None and not is_missing_text(service):
                    merged[key] = service
        else:
            logger.warning(
                "master_df missing AppointmentReason1/Service columns; "
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
