import logging
import re

import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from column_registry import (
    CUSTOM_WEEK_START_DAY,
    DATETIME_COLUMNS,
    TRIM_COLUMNS,
    build_done_map,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# String cleaning
# ---------------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({r"´": "", np.nan: None}, regex=True)
    return df


def trim_string_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Strip leading/trailing whitespace from categorical string columns."""
    if columns is None:
        columns = TRIM_COLUMNS
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df


def capitalize_string_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    if columns is None:
        columns = [
            "PatientFullName",
            "PatientCaseName",
            "ConfirmationStatus",
            "AppointmentReason1",
            "Provider",
            "Service",
            "PatientCasePayerScenario",
        ]
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.title()
    return df


def normalize_confirmation_status(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize ConfirmationStatus to title case, preserving hyphenated words."""
    if "ConfirmationStatus" not in df.columns:
        return df

    df = df.copy()
    normalized = (
        df["ConfirmationStatus"]
        .astype("string")
        .str.strip()
        .str.replace(r"\s*-\s*", "-", regex=True)
        .str.title()
    )
    df["ConfirmationStatus"] = normalized.where(normalized.notna(), None)
    return df


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def format_column_date(df: pd.DataFrame) -> pd.DataFrame:
    """Parse all datetime columns and derive StartOnlyDate."""
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")
            nat_count = df[col].isna().sum()
            if nat_count > 0:
                logger.warning(
                    "Column '%s': %d values could not be parsed (NaT)",
                    col,
                    nat_count,
                )

    # Derive StartOnlyDate as a real date (Timestamp with time=00:00:00)
    if "StartDate" in df.columns:
        df["StartOnlyDate"] = pd.to_datetime(
            df["StartDate"], errors="coerce"
        ).dt.normalize()

    return df


# ---------------------------------------------------------------------------
# Service mapping
# ---------------------------------------------------------------------------

def apply_column_service(
    df: pd.DataFrame, master_df: pd.DataFrame
) -> pd.DataFrame:
    service_map = dict(zip(master_df["AppointmentReason1"], master_df["Service"]))
    existing_service = (
        df["Service"]
        if "Service" in df.columns
        else pd.Series(index=df.index, dtype="object")
    )
    mapped_service = df["AppointmentReason1"].map(service_map)
    df["Service"] = mapped_service.fillna(existing_service)
    return df


# ---------------------------------------------------------------------------
# Done (derived from ConfirmationStatus)
# ---------------------------------------------------------------------------

def apply_column_done(
    df: pd.DataFrame,
    master_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Compute ``Done`` column from ``ConfirmationStatus``.

    Uses the centralized catalog (with ``Check-Out → 1`` as minimum)
    merged with the master sheet mappings.  Unknown statuses default to 0
    and are logged as warnings.
    """
    done_map = build_done_map(master_df)

    mapped = df["ConfirmationStatus"].map(done_map)

    # Log unmapped statuses
    unmapped_mask = mapped.isna() & df["ConfirmationStatus"].notna()
    if unmapped_mask.any():
        unmapped_values = df.loc[unmapped_mask, "ConfirmationStatus"].unique()
        logger.warning(
            "ConfirmationStatus values not in Done catalog (defaulting to 0): %s",
            list(unmapped_values),
        )

    df["Done"] = pd.to_numeric(mapped, errors="coerce").fillna(0).astype(int)
    return df


# ---------------------------------------------------------------------------
# Month
# ---------------------------------------------------------------------------

def apply_column_month(df: pd.DataFrame) -> pd.DataFrame:
    if "StartDate" in df.columns:
        df["Month"] = pd.to_datetime(
            df["StartDate"], errors="coerce"
        ).dt.month
    return df


# ---------------------------------------------------------------------------
# Week (preserved from origin per plan — not recalculated)
# ---------------------------------------------------------------------------

def apply_column_week(df: pd.DataFrame) -> pd.DataFrame:
    if "StartOnlyDate" in df.columns:
        df["Week"] = pd.to_datetime(
            df["StartOnlyDate"], errors="coerce"
        ).dt.isocalendar().week
    return df


# ---------------------------------------------------------------------------
# WeekDate — custom Friday-to-Thursday weeks
# ---------------------------------------------------------------------------

def _compute_friday_thursday_range(date_series: pd.Series) -> pd.Series:
    """
    For each date, compute the Friday–Thursday week range and return
    a formatted string ``MM/DD-MM/DD/YY``.

    The formula to find the Friday that starts the week containing *d*:
        friday = d - timedelta(days=(d.weekday() - CUSTOM_WEEK_START_DAY) % 7)

    Thursday = Friday + 6 days.  The year in the format comes from Thursday.
    """
    dates = pd.to_datetime(date_series, errors="coerce")

    # Compute Friday start for each date
    weekdays = dates.dt.weekday  # Monday=0 … Sunday=6
    offset = (weekdays - CUSTOM_WEEK_START_DAY) % 7
    friday = dates - pd.to_timedelta(offset, unit="D")
    thursday = friday + pd.Timedelta(days=6)

    # Build formatted string  MM/DD-MM/DD/YY
    fri_part = friday.dt.strftime("%m/%d")
    thu_part = thursday.dt.strftime("%m/%d/%y")

    result = fri_part + "-" + thu_part
    # Restore NaT positions as None
    result = result.where(dates.notna(), other=None)
    return result


def apply_column_week_date(df: pd.DataFrame) -> pd.DataFrame:
    """Derive ``WeekDate`` from ``StartDate`` using Fri-Thu weeks."""
    if "StartDate" in df.columns:
        df["WeekDate"] = _compute_friday_thursday_range(df["StartDate"])
    return df


def apply_column_creation_week_date(df: pd.DataFrame) -> pd.DataFrame:
    """Derive ``CreationWeekDate`` from ``CreatedDate`` using Fri-Thu weeks."""
    if "CreatedDate" in df.columns:
        df["CreationWeekDate"] = _compute_friday_thursday_range(df["CreatedDate"])
    return df


# ---------------------------------------------------------------------------
# MonthDate — first day of the month (default assumption)
# ---------------------------------------------------------------------------

def apply_column_month_date(df: pd.DataFrame) -> pd.DataFrame:
    """Derive MonthDate as MM/YY from StartDate.

    BigQuery stores this column as STRING, so we produce a formatted string
    rather than a Timestamp to avoid type-mismatch errors on upload.
    """
    if "StartDate" in df.columns:
        start = pd.to_datetime(df["StartDate"], errors="coerce")
        df["MonthDate"] = start.dt.strftime("%m/%y")
    return df


# ---------------------------------------------------------------------------
# Time — duration in hours
# ---------------------------------------------------------------------------

def apply_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ``Time`` as duration between StartDate and EndDate in hours,
    multiplied by ``Done`` (business rule: non-completed = 0 hours).

    Negative durations are clamped to 0 and logged.
    """
    if "StartDate" in df.columns and "EndDate" in df.columns:
        start = pd.to_datetime(df["StartDate"], errors="coerce")
        end = pd.to_datetime(df["EndDate"], errors="coerce")
        duration_hours = (end - start).dt.total_seconds() / 3600

        # Clamp negative durations
        negative_mask = duration_hours < 0
        if negative_mask.any():
            logger.warning(
                "Found %d rows where EndDate < StartDate; clamping Time to 0",
                negative_mask.sum(),
            )
            duration_hours = duration_hours.clip(lower=0)

        df["Time"] = duration_hours * df["Done"].fillna(0)
    return df


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_WEEK_DATE_RE = re.compile(r"^\d{2}/\d{2}-\d{2}/\d{2}/\d{2}$")


def validate_output_schema(df: pd.DataFrame) -> list[str]:
    """
    Validate the transformed DataFrame against the expected schema.

    Returns a list of warning messages (empty = all OK).
    Warnings are also emitted via the module logger.
    """
    warnings_list: list[str] = []

    def _warn(msg: str):
        warnings_list.append(msg)
        logger.warning("Schema validation: %s", msg)

    # Done ∈ {0, 1}
    if "Done" in df.columns:
        invalid_done = ~df["Done"].isin([0, 1])
        if invalid_done.any():
            _warn(f"Done contains {invalid_done.sum()} values outside {{0, 1}}")

    # Month ∈ [1, 12]
    if "Month" in df.columns:
        month_vals = pd.to_numeric(df["Month"], errors="coerce")
        invalid_month = ~month_vals.between(1, 12) & month_vals.notna()
        if invalid_month.any():
            _warn(f"Month contains {invalid_month.sum()} values outside [1,12]")

    # Time >= 0
    if "Time" in df.columns:
        time_vals = pd.to_numeric(df["Time"], errors="coerce")
        negative_time = time_vals < 0
        if negative_time.any():
            _warn(f"Time contains {negative_time.sum()} negative values")

    # WeekDate format
    if "WeekDate" in df.columns:
        non_null_wd = df["WeekDate"].dropna()
        bad_format = ~non_null_wd.astype(str).str.fullmatch(
            r"\d{2}/\d{2}-\d{2}/\d{2}/\d{2}"
        )
        if bad_format.any():
            _warn(
                f"WeekDate has {bad_format.sum()} values not matching MM/DD-MM/DD/YY"
            )

    # Cross-validation: Month vs StartDate.month
    if "Month" in df.columns and "StartDate" in df.columns:
        sd_month = pd.to_datetime(df["StartDate"], errors="coerce").dt.month
        mismatched = (df["Month"] != sd_month) & sd_month.notna()
        if mismatched.any():
            _warn(
                f"Month does not match StartDate.month in {mismatched.sum()} rows"
            )

    return warnings_list


# ---------------------------------------------------------------------------
# Patient & Insurance enrichment
# ---------------------------------------------------------------------------

def assign_patient_case_name(
    df: pd.DataFrame, patient_df: pd.DataFrame
) -> pd.DataFrame:
    df = df.copy()
    patient_df = patient_df.copy()
    if "PatientID" in df.columns:
        df["_PatientID_numeric"] = pd.to_numeric(df["PatientID"], errors="coerce")
    if "ID" in patient_df.columns:
        patient_df["_ID_numeric"] = pd.to_numeric(patient_df["ID"], errors="coerce")
    df = df.merge(
        patient_df[["ID", "_ID_numeric", "PrimaryInsurancePolicyCompanyName"]],
        left_on="_PatientID_numeric",
        right_on="_ID_numeric",
        how="left",
        suffixes=("", "_patient"),
    )

    df["PatientCaseName"] = (
        df["PatientCaseName"]
        .replace("", pd.NA)
        .fillna(df["PrimaryInsurancePolicyCompanyName"])
        .replace("-", "Selfpay")
    )

    df.drop(
        columns=[
            "ID_patient",
            "PrimaryInsurancePolicyCompanyName",
            "_PatientID_numeric",
            "_ID_numeric",
        ],
        inplace=True,
        errors="ignore",
    )

    df.loc[
        df["PatientCasePayerScenario"].str.contains("Self", na=False),
        "PatientCaseName",
    ] = "Selfpay"
    return df


def assign_case_name_id(
    df: pd.DataFrame, insurance_df: pd.DataFrame
) -> pd.DataFrame:
    insurance_map = (
        insurance_df[["CaseName", "InsuranceID"]]
        .dropna(subset=["CaseName"])
        .drop_duplicates(subset=["CaseName"], keep="last")
        .set_index("CaseName")["InsuranceID"]
    )
    df = df.copy()
    df["CaseNameID"] = df["PatientCaseName"].map(insurance_map)
    return df


# ---------------------------------------------------------------------------
# Identifier normalization
# ---------------------------------------------------------------------------

def normalize_identifier_columns(
    df: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    df = df.copy()
    integer_like_pattern = re.compile(r"^-?\d+\.0+$")

    for col in columns:
        if col not in df.columns:
            continue

        def _normalize(value):
            if pd.isna(value) or value == "":
                return None
            if isinstance(value, (int, np.integer)):
                return str(int(value))
            if isinstance(value, (float, np.floating)) and float(value).is_integer():
                return str(int(value))

            text = str(value).strip()
            if integer_like_pattern.fullmatch(text):
                return text.split(".", 1)[0]
            return text

        df[col] = df[col].map(_normalize)

    return df


# ---------------------------------------------------------------------------
# Deleted-appointment handling
# ---------------------------------------------------------------------------

def find_appointments_not_in_tebra(
    tebra_df: pd.DataFrame, sheet_df: pd.DataFrame
) -> list:
    tebra_ids = set(tebra_df["ID"].dropna().astype(str))
    sheet_ids = set(sheet_df["ID"].dropna().astype(str))
    return list(sheet_ids - tebra_ids)


def mark_deleted_appointments(
    df: pd.DataFrame, deleted_ids: list
) -> pd.DataFrame:
    if "ID" not in df.columns:
        return df
    df = df.copy()
    df.loc[df["ID"].isin(deleted_ids), "ConfirmationStatus"] = "Deleted"
    df.loc[df["ConfirmationStatus"] == "Deleted", "Done"] = 0
    df.loc[df["ConfirmationStatus"] == "Deleted", "Time"] = 0
    return df


# ---------------------------------------------------------------------------
# Final formatting (for Sheets/BQ export)
# ---------------------------------------------------------------------------

def format_datetime_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    fmt: str = "%m/%d/%Y %H:%M:%S",
) -> pd.DataFrame:
    if columns is None:
        columns = ["StartDate", "EndDate", "CreatedDate", "LastModifiedDate", "StartOnlyDate"]
    for col in columns:
        if col in df.columns:
            if col == "StartOnlyDate":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%m/%d/%Y")
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime(fmt)
    return df


# ---------------------------------------------------------------------------
# Orchestrator — single entry point for all transformations
# ---------------------------------------------------------------------------

def apply_all_transformations(
    df: pd.DataFrame,
    master_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply all transformations in the correct dependency order.

    Execution order:
    1.  clean_dataframe          — remove special chars / NaN→None
    2.  trim_string_columns      — strip whitespace
    3.  normalize_confirmation_status
    4.  capitalize_string_columns
    5.  format_column_date       — parse ALL datetimes + derive StartOnlyDate
    6.  apply_column_service     — map Service from master
    7.  apply_column_done        — map Done with local+master catalog
    8.  apply_column_month       — derive Month from StartDate
    9.  apply_column_week_date   — derive WeekDate (Fri-Thu)
    10. apply_column_creation_week_date — derive CreationWeekDate from CreatedDate
    11. apply_column_month_date  — derive MonthDate (1st of month)
    12. apply_column_week        — derive Week (ISO, preserved)
    13. apply_time_column        — derive Time (duration * Done)
    14. drop duplicates
    15. validate_output_schema   — log violations
    """
    df = clean_dataframe(df)
    df = trim_string_columns(df)
    df = normalize_confirmation_status(df)
    df = capitalize_string_columns(df)
    df = format_column_date(df)
    df = apply_column_service(df, master_df)
    df = apply_column_done(df, master_df)
    df = apply_column_month(df)
    df = apply_column_week_date(df)
    df = apply_column_creation_week_date(df)
    df = apply_column_month_date(df)
    df = apply_column_week(df)
    df = apply_time_column(df)
    df.drop_duplicates(inplace=True)
    validate_output_schema(df)
    return df


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def generate_date_range(
    months_back: int = 1, months_forward: int = 1, freq_days: int = 8
) -> list:
    from_date = datetime.now() - relativedelta(months=months_back)
    until_date = datetime.now() + relativedelta(months=months_forward)
    return (
        pd.date_range(from_date, until_date, freq=f"{freq_days}D")
        .strftime("%m/%d/%Y")
        .tolist()
    )
