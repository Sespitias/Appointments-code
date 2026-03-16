import logging
import re
from typing import Literal

import pandas as pd
import pandas_gbq
from google.api_core.exceptions import Forbidden, GoogleAPIError, NotFound
from google.cloud import bigquery


BQ_COLUMNS = [
    "ID",
    "ConfirmationStatus",
    "CreatedDate",
    "LastModifiedDate",
    "ServiceLocationName",
    "PatientID",
    "PatientFullName",
    "PatientCaseID",
    "PatientCaseName",
    "PatientCasePayerScenario",
    "StartDate",
    "EndDate",
    "AppointmentReason1",
    "Provider",
    "Service",
    "Done",
    "StartOnlyDate",
    "WeekDate",
    "MonthDate",
    "Month",
    "Week",
    "Time",
    "CreationWeekDate",
    "CaseNameID",
]

CONVERSION_MAPPING = {
    "ID": str,
    "ConfirmationStatus": str,
    "CreatedDate": lambda x: pd.to_datetime(x, errors="coerce", utc=True),
    "LastModifiedDate": lambda x: pd.to_datetime(x, errors="coerce", utc=True),
    "ServiceLocationName": str,
    "PatientID": str,
    "PatientFullName": str,
    "PatientCaseID": str,
    "PatientCaseName": str,
    "PatientCasePayerScenario": str,
    "StartDate": lambda x: pd.to_datetime(x, errors="coerce", utc=True),
    "EndDate": lambda x: pd.to_datetime(x, errors="coerce", utc=True),
    "AppointmentReason1": str,
    "Provider": str,
    "Service": str,
    "Done": lambda x: pd.to_numeric(x, errors="coerce", downcast="integer"),
    "StartOnlyDate": lambda x: pd.to_datetime(x, errors="coerce").dt.date,
    "WeekDate": str,
    "MonthDate": str,
    "Month": lambda x: pd.to_numeric(x, errors="coerce", downcast="integer"),
    "Week": lambda x: pd.to_numeric(x, errors="coerce", downcast="integer"),
    "Time": lambda x: pd.to_numeric(x, errors="coerce"),
    "CreationWeekDate": str,
    "CaseNameID": str,
}

logger = logging.getLogger(__name__)
_BQ_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_identifier(value: str, label: str) -> str:
    if not value or not _BQ_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Invalid BigQuery {label}: {value!r}")
    return value


def validate_bigquery_destination(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> str:
    _validate_identifier(dataset_id, "dataset_id")
    _validate_identifier(table_name, "table_name")

    dataset_ref = f"{project_id}.{dataset_id}"
    table_ref = f"{dataset_ref}.{table_name}"

    try:
        client.get_dataset(dataset_ref)
    except Forbidden as exc:
        raise PermissionError(
            f"Forbidden: missing access to dataset {dataset_ref}"
        ) from exc
    except NotFound as exc:
        raise FileNotFoundError(
            f"BigQuery dataset not found: {dataset_ref}"
        ) from exc

    try:
        client.get_table(table_ref)
    except Forbidden as exc:
        raise PermissionError(
            f"Forbidden: missing access to table {table_ref}"
        ) from exc
    except NotFound as exc:
        raise FileNotFoundError(
            f"BigQuery table not found: {table_ref}"
        ) from exc

    return table_ref


def load_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_name: str,
    df: pd.DataFrame,
    if_exists: Literal["fail", "replace", "append"],
    credentials,
) -> None:
    _validate_identifier(dataset_id, "dataset_id")
    _validate_identifier(table_name, "table_name")
    table_ref = f"{dataset_id}.{table_name}"
    pandas_gbq.to_gbq(
        df,
        table_ref,
        project_id=project_id,
        if_exists=if_exists,
        credentials=credentials,
    )


def convert_columns(df: pd.DataFrame, mapping: dict = CONVERSION_MAPPING) -> pd.DataFrame:
    result = df.copy()
    for col, conversion in mapping.items():
        if col in result.columns:
            try:
                if callable(conversion) and not isinstance(conversion, type):
                    result[col] = conversion(result[col])
                else:
                    result[col] = result[col].astype(conversion)
            except Exception as exc:
                raise ValueError(f"Error converting column '{col}': {exc}") from exc
    return result


def filter_recent_appointments(df: pd.DataFrame, days_back: int = 10) -> pd.DataFrame:
    if "LastModifiedDate" not in df.columns:
        raise KeyError("Missing required column: LastModifiedDate")

    df = df.copy()
    df["LastModifiedDate"] = pd.to_datetime(df["LastModifiedDate"], format="mixed", utc=True)
    today = pd.Timestamp.today(tz="UTC")
    return df[
        df["LastModifiedDate"].between(
            today.floor("D") - pd.Timedelta(days=days_back),
            today.ceil("D"),
            inclusive="both",
        )
    ]


def merge_appointments(
    project_id: str,
    dataset_id: str,
    source_table: str,
    target_table: str,
    client: bigquery.Client,
) -> bool:
    _validate_identifier(dataset_id, "dataset_id")
    _validate_identifier(source_table, "source_table")
    _validate_identifier(target_table, "target_table")

    query = f"""
    MERGE INTO `{dataset_id}.{target_table}` AS target
    USING (
        SELECT * EXCEPT(rn)
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY LastModifiedDate DESC, StartDate DESC) AS rn
            FROM `{dataset_id}.{source_table}`
        )
        WHERE rn = 1
    ) AS source
    ON target.ID = source.ID
    WHEN MATCHED AND (
        MD5(CONCAT(
            COALESCE(CAST(source.ConfirmationStatus AS STRING), ''),
            COALESCE(CAST(source.CreatedDate AS STRING), ''),
            COALESCE(CAST(source.LastModifiedDate AS STRING), ''),
            COALESCE(CAST(source.ServiceLocationName AS STRING), ''),
            COALESCE(CAST(source.PatientID AS STRING), ''),
            COALESCE(CAST(source.PatientFullName AS STRING), ''),
            COALESCE(CAST(source.PatientCaseName AS STRING), ''),
            COALESCE(CAST(source.PatientCasePayerScenario AS STRING), ''),
            COALESCE(CAST(source.StartDate AS STRING), ''),
            COALESCE(CAST(source.EndDate AS STRING), ''),
            COALESCE(CAST(source.AppointmentReason1 AS STRING), ''),
            COALESCE(CAST(source.Provider AS STRING), ''),
            COALESCE(CAST(source.Service AS STRING), ''),
            COALESCE(CAST(source.Done AS STRING), ''),
            COALESCE(CAST(source.StartOnlyDate AS STRING), ''),
            COALESCE(CAST(source.WeekDate AS STRING), ''),
            COALESCE(CAST(source.MonthDate AS STRING), ''),
            COALESCE(CAST(source.Month AS STRING), ''),
            COALESCE(CAST(source.Week AS STRING), ''),
            COALESCE(CAST(source.Time AS STRING), ''),
            COALESCE(CAST(source.CreationWeekDate AS STRING), '')
        )) != MD5(CONCAT(
            COALESCE(CAST(target.ConfirmationStatus AS STRING), ''),
            COALESCE(CAST(target.CreatedDate AS STRING), ''),
            COALESCE(CAST(target.LastModifiedDate AS STRING), ''),
            COALESCE(CAST(target.ServiceLocationName AS STRING), ''),
            COALESCE(CAST(target.PatientID AS STRING), ''),
            COALESCE(CAST(target.PatientFullName AS STRING), ''),
            COALESCE(CAST(target.PatientCaseName AS STRING), ''),
            COALESCE(CAST(target.PatientCasePayerScenario AS STRING), ''),
            COALESCE(CAST(target.StartDate AS STRING), ''),
            COALESCE(CAST(target.EndDate AS STRING), ''),
            COALESCE(CAST(target.AppointmentReason1 AS STRING), ''),
            COALESCE(CAST(target.Provider AS STRING), ''),
            COALESCE(CAST(target.Service AS STRING), ''),
            COALESCE(CAST(target.Done AS STRING), ''),
            COALESCE(CAST(target.StartOnlyDate AS STRING), ''),
            COALESCE(CAST(target.WeekDate AS STRING), ''),
            COALESCE(CAST(target.MonthDate AS STRING), ''),
            COALESCE(CAST(target.Month AS STRING), ''),
            COALESCE(CAST(target.Week AS STRING), ''),
            COALESCE(CAST(target.Time AS STRING), ''),
            COALESCE(CAST(target.CreationWeekDate AS STRING), '')
        ))
    )
    THEN UPDATE SET
        ConfirmationStatus = source.ConfirmationStatus,
        CreatedDate = source.CreatedDate,
        LastModifiedDate = source.LastModifiedDate,
        ServiceLocationName = source.ServiceLocationName,
        PatientID = source.PatientID,
        PatientFullName = source.PatientFullName,
        PatientCaseID = source.PatientCaseID,
        PatientCaseName = source.PatientCaseName,
        PatientCasePayerScenario = source.PatientCasePayerScenario,
        StartDate = source.StartDate,
        EndDate = source.EndDate,
        AppointmentReason1 = source.AppointmentReason1,
        Provider = source.Provider,
        Service = source.Service,
        Done = source.Done,
        StartOnlyDate = source.StartOnlyDate,
        WeekDate = source.WeekDate,
        MonthDate = source.MonthDate,
        Month = source.Month,
        Week = source.Week,
        Time = source.Time,
        CreationWeekDate = source.CreationWeekDate,
        CaseNameID = source.CaseNameID
    WHEN NOT MATCHED THEN INSERT (
        {', '.join(BQ_COLUMNS)}
    ) VALUES (
        source.ID, source.ConfirmationStatus, source.CreatedDate, source.LastModifiedDate,
        source.ServiceLocationName, source.PatientID, source.PatientFullName, source.PatientCaseID,
        source.PatientCaseName, source.PatientCasePayerScenario, source.StartDate, source.EndDate,
        source.AppointmentReason1, source.Provider, source.Service, source.Done, source.StartOnlyDate,
        source.WeekDate, source.MonthDate, source.Month, source.Week, source.Time, source.CreationWeekDate,
        source.CaseNameID
    )
    """

    try:
        job = client.query(query, project=project_id)
        job.result()
        logger.info("BigQuery merge completed")
        return True
    except GoogleAPIError as exc:
        logger.exception("BigQuery error during merge: %s", exc)
        raise
    except Exception as exc:
        logger.exception("Unexpected error during BigQuery merge: %s", exc)
        raise
