"""
Main orchestration script for Appointment extraction from Tebra.
Designed for Google Cloud Run deployment.
"""

import logging
import os
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from bigquery_ops import (
    BQ_COLUMNS,
    convert_columns,
    filter_recent_appointments,
    load_to_bigquery,
    merge_appointments,
    validate_bigquery_destination,
)
from config_loader import get_config
from sheets_ops import SheetsClient, get_sheet_data
from tebra_client import TebraClient
from transformations import (
    apply_all_transformations,
    apply_column_done,
    apply_time_column,
    assign_case_name_id,
    assign_patient_case_name,
    find_appointments_not_in_tebra,
    format_datetime_columns,
    generate_date_range,
    mark_deleted_appointments,
    normalize_identifier_columns,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]

EXPORT_APPOINTMENT_COLUMNS = BQ_COLUMNS.copy()
IDENTIFIER_COLUMNS = ["ID", "PatientID", "PatientCaseID", "CaseNameID"]
EXPORT_DATETIME_COLUMNS = ["StartDate", "EndDate", "CreatedDate", "LastModifiedDate"]


class AppointmentPipeline:
    def __init__(self, config):
        self.config = config
        self.credentials = None
        self.bigquery_client = None
        self.tebra_client = None
        self.sheets_client = None
        self.appointment_df = None
        self.master_df = None
        self.patient_df = None
        self.insurance_df = None

    def authenticate(self):
        service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_path:
            return self._build_service_account_credentials(service_account_path)
        return self._build_oauth_credentials()

    @staticmethod
    def _build_service_account_credentials(creds_path: str):
        from google.oauth2 import service_account

        path = Path(creds_path)
        if not path.exists():
            raise FileNotFoundError(
                f"GOOGLE_APPLICATION_CREDENTIALS points to a missing file: {creds_path}"
            )

        return service_account.Credentials.from_service_account_file(
            path,
            scopes=GOOGLE_SCOPES,
        )

    @staticmethod
    def _build_oauth_credentials():
        from google.auth.exceptions import RefreshError
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        refresh_token = os.getenv("REFRESH_TOKEN")

        missing = [
            name
            for name, value in (
                ("CLIENT_ID", client_id),
                ("CLIENT_SECRET", client_secret),
                ("REFRESH_TOKEN", refresh_token),
            )
            if not value
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise RuntimeError(
                "Missing Google OAuth configuration. Set GOOGLE_APPLICATION_CREDENTIALS "
                f"or provide: {missing_str}"
            )

        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=GOOGLE_SCOPES,
        )
        try:
            credentials.refresh(Request())
        except RefreshError as exc:
            raise RuntimeError(
                "OAuth refresh token rejected the requested scopes. "
                "Regenerate the token with Sheets, Drive and BigQuery/Cloud Platform scopes, "
                "or use GOOGLE_APPLICATION_CREDENTIALS with a service account."
            ) from exc

        return credentials

    def init_sheets_client(self, credentials):
        import gspread

        gc = gspread.auth.authorize(credentials)
        self.sheets_client = SheetsClient(gc)
        return gc

    def init_bigquery_client(self, credentials):
        project_id = self.config.bigquery["project_id"]
        self.bigquery_client = bigquery.Client(
            project=project_id,
            credentials=credentials,
            location="us",
        )
        return self.bigquery_client

    def init_tebra_client(self, client):
        tebra_config = self.config.tebra
        request_header = {
            "User": tebra_config["user"],
            "Password": tebra_config["password"],
            "CustomerKey": tebra_config["customer_key"],
        }
        self.tebra_client = TebraClient(client, request_header)

    def load_master_data(self, gc):
        logger.info("Loading master data...")

        sheets_config = self.config.google_sheets

        self.master_df = get_sheet_data(sheets_config["sheet_key"], "Master", gc)
        self.master_df = self.master_df[["Reason", "Service", "Status", "Active"]].drop_duplicates()
        self.master_df.rename(
            columns={
                "Status": "ConfirmationStatus",
                "Active": "Done",
                "Reason": "AppointmentReason1",
            },
            inplace=True,
        )

        self.patient_df = get_sheet_data(
            sheets_config["patient_sheet_key"], "PatientsInformation", gc
        )
        self.patient_df = self.patient_df[["ID", "PrimaryInsurancePolicyCompanyName"]].replace("", None)

        self.insurance_df = get_sheet_data(
            sheets_config["insurance_sheet_key"], "Insurances_Names", gc
        )
        self.insurance_df.replace("", None, inplace=True)

        logger.info(
            "Master: %s, Patients: %s, Insurances: %s",
            len(self.master_df),
            len(self.patient_df),
            len(self.insurance_df),
        )

    def load_sheet_appointments(self, gc):
        logger.info("Loading sheet appointments...")
        sheets_config = self.config.google_sheets
        return get_sheet_data(sheets_config["sheet_key"], "Appointment", gc)

    def extract_tebra_appointments(self, parallel: bool = True):
        logger.info("Extracting appointments from Tebra...")

        pipeline_config = self.config.pipeline
        dates = generate_date_range(
            months_back=pipeline_config["months_back"],
            months_forward=pipeline_config["months_forward"],
            freq_days=pipeline_config["fetch_interval_days"],
        )

        if parallel:
            self.appointment_df = self.tebra_client.fetch_appointments_parallel(
                dates, pipeline_config["parallel_workers"]
            )
        else:
            self.appointment_df = self.tebra_client.fetch_appointments_sequential(dates)

        self.appointment_df.dropna(subset=["ID"], inplace=True)
        return self.appointment_df

    def process_appointments(self, sheet_appointments_df):
        logger.info("Processing appointments...")

        transformed_df = self._prepare_tebra_appointments()
        extraction_start, extraction_end = self._get_extraction_bounds()
        relevant_sheet_df = self._filter_sheet_appointments_by_window(
            sheet_appointments_df,
            extraction_start,
            extraction_end,
        )
        deleted_ids = find_appointments_not_in_tebra(transformed_df, relevant_sheet_df)
        impacted_ids = self._build_impacted_ids(transformed_df, deleted_ids)
        impacted_sheet_df = self._filter_sheet_appointments_by_ids(
            sheet_appointments_df,
            impacted_ids,
        )
        merged_df = self._merge_with_sheet_appointments(transformed_df, impacted_sheet_df)
        export_df = self._build_appointment_import_df(merged_df, EXPORT_APPOINTMENT_COLUMNS)
        export_df = self._finalize_export_appointments(export_df, deleted_ids)
        return export_df

    def _prepare_tebra_appointments(self) -> pd.DataFrame:
        df = self.appointment_df.copy()
        df = df.replace({r"´": "", None: ""}, regex=True)
        df = apply_all_transformations(df, self.master_df)
        return df.replace("", None).dropna(subset=["ID"])

    def _get_extraction_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        pipeline_config = self.config.pipeline
        now = pd.Timestamp.now()
        start = now - pd.DateOffset(months=pipeline_config["months_back"])
        end = now + pd.DateOffset(months=pipeline_config["months_forward"])
        return start.normalize(), end.normalize()

    @staticmethod
    def _build_impacted_ids(tebra_df: pd.DataFrame, deleted_ids: list[str]) -> set[str]:
        tebra_ids = set(tebra_df["ID"].dropna().astype(str))
        return tebra_ids.union({str(appointment_id) for appointment_id in deleted_ids})

    @staticmethod
    def _filter_sheet_appointments_by_ids(
        sheet_appointments_df: pd.DataFrame,
        impacted_ids: set[str],
    ) -> pd.DataFrame:
        if sheet_appointments_df.empty or not impacted_ids:
            return sheet_appointments_df.iloc[0:0].copy()

        comparable_ids = sheet_appointments_df["ID"].astype(str)
        return sheet_appointments_df.loc[comparable_ids.isin(impacted_ids)].copy()

    @staticmethod
    def _filter_sheet_appointments_by_window(
        sheet_appointments_df: pd.DataFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
        date_column: str = "StartDate",
    ) -> pd.DataFrame:
        if sheet_appointments_df.empty or date_column not in sheet_appointments_df.columns:
            return sheet_appointments_df.iloc[0:0].copy()

        sheet_dates = pd.to_datetime(
            sheet_appointments_df[date_column],
            format="mixed",
            errors="coerce",
        )
        mask = sheet_dates.between(start, end, inclusive="both")
        return sheet_appointments_df.loc[mask].copy()

    @staticmethod
    def _merge_with_sheet_appointments(
        tebra_df: pd.DataFrame, sheet_appointments_df: pd.DataFrame
    ) -> pd.DataFrame:
        return tebra_df.merge(
            sheet_appointments_df,
            on="ID",
            how="outer",
            suffixes=("_tebra", "_sheet"),
        )

    def _finalize_export_appointments(
        self, import_df: pd.DataFrame, deleted_ids: list[str]
    ) -> pd.DataFrame:
        export_df = import_df.dropna(subset=["ID"]).drop_duplicates(subset=["ID"], keep="last")

        for col in EXPORT_DATETIME_COLUMNS:
            if col in export_df.columns:
                export_df[col] = pd.to_datetime(export_df[col], format="mixed", errors="coerce")

        export_df = apply_column_done(export_df, self.master_df)
        export_df = apply_time_column(export_df)
        export_df = mark_deleted_appointments(export_df, deleted_ids)
        export_df = assign_patient_case_name(export_df, self.patient_df)
        export_df = assign_case_name_id(export_df, self.insurance_df)
        export_df = normalize_identifier_columns(export_df, IDENTIFIER_COLUMNS)
        export_df = format_datetime_columns(export_df)
        return export_df

    @staticmethod
    def _normalize_for_delta(df: pd.DataFrame) -> pd.DataFrame:
        comparable = df.copy()
        for col in EXPORT_APPOINTMENT_COLUMNS:
            if col not in comparable.columns:
                comparable[col] = None
        comparable = comparable[EXPORT_APPOINTMENT_COLUMNS]
        comparable = comparable.where(~comparable.isna(), "")
        return comparable.astype(str)

    @staticmethod
    def _build_row_hashes(df: pd.DataFrame) -> pd.DataFrame:
        comparable = AppointmentPipeline._normalize_for_delta(df)
        comparable["_hash"] = pd.util.hash_pandas_object(
            comparable.drop(columns=["ID"]),
            index=False,
        )
        return comparable[["ID", "_hash"]].drop_duplicates(subset=["ID"], keep="last")

    def build_delta_appointments(
        self, processed_df: pd.DataFrame, sheet_appointments_df: pd.DataFrame
    ) -> pd.DataFrame:
        if processed_df.empty:
            return processed_df.copy()

        processed_hashes = self._build_row_hashes(processed_df)
        existing_hashes = self._build_row_hashes(sheet_appointments_df)

        merged = processed_hashes.merge(
            existing_hashes.rename(columns={"_hash": "_hash_existing"}),
            on="ID",
            how="left",
        )

        changed_mask = (
            merged["_hash_existing"].isna()
            | (merged["_hash"] != merged["_hash_existing"])
        )
        delta_ids = merged.loc[changed_mask, "ID"].tolist()

        if not delta_ids:
            return processed_df.iloc[0:0].copy()

        delta_df = processed_df[processed_df["ID"].isin(delta_ids)].copy()
        return delta_df.drop_duplicates(subset=["ID"], keep="last")

    @staticmethod
    def _build_appointment_import_df(merged_df: pd.DataFrame, sheet_columns: list[str]) -> pd.DataFrame:
        import_df = pd.DataFrame({"ID": merged_df["ID"]})

        for col in sheet_columns:
            if col == "ID":
                continue

            tebra_col = f"{col}_tebra"
            sheet_col = f"{col}_sheet"

            if tebra_col in merged_df.columns and sheet_col in merged_df.columns:
                tebra_values = merged_df[tebra_col]
                sheet_values = merged_df[sheet_col]
                merged_values = pd.Series(
                    sheet_values.to_numpy(dtype=object, copy=True),
                    index=merged_df.index,
                    dtype=object,
                )
                tebra_mask = ~tebra_values.isna()
                merged_values.loc[tebra_mask] = tebra_values.loc[tebra_mask].astype(object)
                import_df[col] = merged_values
            elif tebra_col in merged_df.columns:
                import_df[col] = merged_df[tebra_col]
            elif sheet_col in merged_df.columns:
                import_df[col] = merged_df[sheet_col]

        for required_col in EXPORT_APPOINTMENT_COLUMNS:
            if required_col not in import_df.columns:
                import_df[required_col] = None

        return import_df[EXPORT_APPOINTMENT_COLUMNS]

    def upload_to_bigquery(self, df: pd.DataFrame):
        logger.info("Uploading to BigQuery...")

        bq_config = self.config.bigquery
        exec_config = self.config.execution

        df_filtered = filter_recent_appointments(df, exec_config["days_back_filter"])
        logger.info("Filtered %s recent records", len(df_filtered))
        if df_filtered.empty:
            logger.info("No delta rows to upload to BigQuery")
            return

        validate_bigquery_destination(
            client=self.bigquery_client,
            project_id=bq_config["project_id"],
            dataset_id=bq_config["dataset_id"],
            table_name=bq_config["source_table"],
        )

        df_converted = convert_columns(df_filtered)

        load_to_bigquery(
            project_id=bq_config["project_id"],
            dataset_id=bq_config["dataset_id"],
            table_name=bq_config["source_table"],
            df=df_converted,
            if_exists="replace",
            credentials=self.credentials,
        )

        merge_appointments(
            project_id=bq_config["project_id"],
            dataset_id=bq_config["dataset_id"],
            source_table=bq_config["source_table"],
            target_table=bq_config["target_table"],
            client=self.bigquery_client,
        )

    def upload_to_sheets(self, df: pd.DataFrame):
        logger.info("Uploading to Google Sheets...")

        sheets_config = self.config.google_sheets
        spreadsheet = self.sheets_client.open_spreadsheet(sheets_config["sheet_key"])

        upload_ok = self.sheets_client.upsert_dataframe(
            spreadsheet,
            df,
            "Appointment",
        )
        if not upload_ok:
            raise RuntimeError("Google Sheets upload failed")

    def run(self):
        try:
            config_errors = self.config.validate()
            if config_errors:
                for error in config_errors:
                    logger.error(error)
                sys.exit(1)

            self.credentials = self.authenticate()
            gc = self.init_sheets_client(self.credentials)
            self.init_bigquery_client(self.credentials)

            from zeep import Client

            tebra_config = self.config.tebra
            client = Client(tebra_config["wsdl"])
            self.init_tebra_client(client)

            self.load_master_data(gc)
            sheet_appointments = self.load_sheet_appointments(gc)
            self.extract_tebra_appointments(parallel=True)

            processed_df = self.process_appointments(sheet_appointments)
            delta_df = self.build_delta_appointments(processed_df, sheet_appointments)
            logger.info("Delta rows detected for upload: %s", len(delta_df))

            if delta_df.empty:
                logger.info("No new or changed appointments detected")
            else:
                self.upload_to_bigquery(delta_df)
                self.upload_to_sheets(delta_df)

            logger.info("Pipeline completed successfully!")
            return True
        except Exception as e:
            logger.exception("Pipeline failed: %s", e)
            return False


def main():
    config = get_config()
    pipeline = AppointmentPipeline(config)
    success = pipeline.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
