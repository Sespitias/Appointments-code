import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from google.auth.exceptions import RefreshError
from google.api_core.exceptions import Forbidden, NotFound

from bigquery_ops import (
    BQ_COLUMNS,
    filter_recent_appointments,
    load_to_bigquery,
    validate_bigquery_destination,
)
from main import AppointmentPipeline
from sheets_ops import SheetsClient
from transformations import (
    apply_column_done,
    apply_column_service,
    apply_column_week_date,
    apply_column_creation_week_date,
    apply_time_column,
    assign_patient_case_name,
    format_column_date,
    normalize_identifier_columns,
    trim_string_columns,
    validate_output_schema,
)
from column_registry import build_done_map


class AppointmentPipelineTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(
            bigquery={
                "project_id": "weekly-revenue-integration",
                "dataset_id": "Telemind_BD",
                "source_table": "appointment_update",
                "target_table": "appointment_prod",
            },
            google_sheets={
                "sheet_key": "sheet",
                "patient_sheet_key": "patient_sheet",
                "insurance_sheet_key": "insurance_sheet",
            },
            pipeline={
                "months_back": 1,
                "months_forward": 1,
                "fetch_interval_days": 8,
                "parallel_workers": 5,
                "async_upload": False,
                "chunk_size": 100,
            },
            execution={"days_back_filter": 10},
            tebra={"user": "u", "password": "p", "customer_key": "k", "wsdl": "http://example.com"},
            validate=lambda: [],
        )
        self.pipeline = AppointmentPipeline(self.config)
        self.pipeline.master_df = pd.DataFrame(
            {
                "AppointmentReason1": ["Consultation", "Follow Up", "Deleted"],
                "Service": ["Consult", "Control", "Deleted Service"],
                "ConfirmationStatus": ["Confirmed", "Completed", "Deleted"],
                "Done": [0, 1, 0],
            }
        )
        self.pipeline.patient_df = pd.DataFrame(
            {"ID": [10, 11], "PrimaryInsurancePolicyCompanyName": ["Aetna", "Cigna"]}
        )
        self.pipeline.insurance_df = pd.DataFrame(
            {"CaseName": ["Aetna", "Cigna", "Selfpay"], "InsuranceID": ["100", "200", "999"]}
        )

    def test_build_import_df_preserves_sheet_only_rows_and_id(self):
        merged_df = pd.DataFrame(
            {
                "ID": ["1", "2"],
                "ConfirmationStatus_tebra": ["Confirmed", None],
                "ConfirmationStatus_sheet": ["Old", "Keep"],
                "PatientID_tebra": ["10", None],
                "PatientID_sheet": ["10", "11"],
                "StartDate_tebra": ["2024-01-01 09:00:00", None],
                "StartDate_sheet": ["2024-01-01 08:00:00", "2024-01-02 10:00:00"],
            }
        )

        result = self.pipeline._build_appointment_import_df(
            merged_df, ["ID", "ConfirmationStatus", "PatientID", "StartDate"]
        )

        self.assertEqual(result["ID"].tolist(), ["1", "2"])
        self.assertEqual(result.loc[0, "ConfirmationStatus"], "Confirmed")
        self.assertEqual(result.loc[1, "ConfirmationStatus"], "Keep")
        self.assertEqual(result.loc[1, "PatientID"], "11")
        self.assertEqual(result.loc[1, "StartDate"], "2024-01-02 10:00:00")

    def test_build_import_df_uses_canonical_export_columns(self):
        merged_df = pd.DataFrame(
            {
                "ID": ["1"],
                "ConfirmationStatus_tebra": ["Confirmed"],
                "CaseNameID_tebra": ["123"],
                "Legacy_sheet": ["old value"],
            }
        )

        result = self.pipeline._build_appointment_import_df(merged_df, BQ_COLUMNS)

        self.assertEqual(result.columns.tolist(), BQ_COLUMNS)
        self.assertEqual(result.loc[0, "CaseNameID"], "123")

    def test_build_import_df_handles_mixed_date_formats_without_coercion_error(self):
        merged_df = pd.DataFrame(
            {
                "ID": ["1", "2"],
                "CreatedDate_tebra": [pd.Timestamp("2026-03-05 09:00:00"), pd.NaT],
                "CreatedDate_sheet": ["03/05/2026 09:00:00", "3/5/2026"],
            }
        )

        result = self.pipeline._build_appointment_import_df(merged_df, ["ID", "CreatedDate"])

        self.assertEqual(result.loc[0, "CreatedDate"], pd.Timestamp("2026-03-05 09:00:00"))
        self.assertEqual(pd.to_datetime(result.loc[1, "CreatedDate"]), pd.Timestamp("2026-03-05 00:00:00"))

    def test_build_import_df_handles_nullable_integer_columns_without_cast_error(self):
        merged_df = pd.DataFrame(
            {
                "ID": ["1", "2"],
                "Week_tebra": pd.Series([10, pd.NA], dtype="UInt32"),
                "Week_sheet": pd.Series([10.0, 11.0], dtype="float64"),
            }
        )

        result = self.pipeline._build_appointment_import_df(merged_df, ["ID", "Week"])

        self.assertEqual(result.loc[0, "Week"], 10)
        self.assertEqual(result.loc[1, "Week"], 11.0)

    def test_process_appointments_marks_deleted_rows_and_keeps_case_mapping(self):
        self.pipeline.appointment_df = pd.DataFrame(
            {
                "ID": ["1"],
                "ConfirmationStatus": ["Confirmed"],
                "CreatedDate": ["2024-01-01 08:00:00"],
                "LastModifiedDate": ["2024-01-02 08:00:00"],
                "ServiceLocationName": ["HQ"],
                "PatientID": ["10"],
                "PatientFullName": ["john doe"],
                "PatientCaseID": ["500"],
                "PatientCaseName": [None],
                "PatientCasePayerScenario": ["Insurance"],
                "StartDate": ["2024-01-03 09:00:00"],
                "EndDate": ["2024-01-03 10:00:00"],
                "AppointmentReason1": ["Consultation"],
                "Provider": ["doctor who"],
            }
        )
        sheet_df = pd.DataFrame(
            {
                "ID": ["1", "2"],
                "ConfirmationStatus": ["Old", "Completed"],
                "CreatedDate": ["2024-01-01 08:00:00", "2024-01-04 08:00:00"],
                "LastModifiedDate": ["2024-01-02 08:00:00", "2024-01-05 08:00:00"],
                "ServiceLocationName": ["HQ", "Branch"],
                "PatientID": ["10", "11"],
                "PatientFullName": ["John Doe", "Jane Roe"],
                "PatientCaseID": ["500", "501"],
                "PatientCaseName": ["", ""],
                "PatientCasePayerScenario": ["Insurance", "Self"],
                "StartDate": ["2024-01-03 09:00:00", "2024-01-06 11:00:00"],
                "EndDate": ["2024-01-03 10:00:00", "2024-01-06 12:00:00"],
                "AppointmentReason1": ["Consultation", "Follow Up"],
                "Provider": ["Doctor Who", "Doctor Two"],
                "Service": ["", "Legacy"],
                "Done": [0, 1],
                "StartOnlyDate": ["2024-01-03", "2024-01-06"],
                "WeekDate": ["12/29-01/04/24", "01/05-01/11/24"],
                "MonthDate": ["2024-01-01", "2024-01-01"],
                "Month": [1, 1],
                "Week": [1, 1],
                "Time": [0.0, 1.0],
                "CreationWeekDate": ["12/29-01/04/24", "01/05-01/11/24"],
            }
        )

        with patch.object(
            self.pipeline,
            "_get_extraction_bounds",
            return_value=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")),
        ):
            result = self.pipeline.process_appointments(sheet_df).sort_values("ID").reset_index(drop=True)

        self.assertEqual(result["ID"].tolist(), ["1", "2"])
        self.assertEqual(result.loc[0, "Service"], "Consult")
        self.assertEqual(result.loc[0, "PatientCaseName"], "Aetna")
        self.assertEqual(result.loc[0, "CaseNameID"], "100")
        self.assertEqual(result.loc[1, "ConfirmationStatus"], "Deleted")
        self.assertEqual(result.loc[1, "Done"], 0)
        self.assertEqual(result.loc[1, "Time"], 0)
        self.assertEqual(result.loc[1, "PatientCaseName"], "Selfpay")
        self.assertEqual(result.loc[1, "CaseNameID"], "999")

    def test_process_appointments_only_returns_impacted_rows(self):
        self.pipeline.appointment_df = pd.DataFrame(
            {
                "ID": ["1"],
                "ConfirmationStatus": ["Confirmed"],
                "CreatedDate": ["2024-01-01 08:00:00"],
                "LastModifiedDate": ["2024-01-02 08:00:00"],
                "ServiceLocationName": ["HQ"],
                "PatientID": ["10"],
                "PatientFullName": ["john doe"],
                "PatientCaseID": ["500"],
                "PatientCaseName": [None],
                "PatientCasePayerScenario": ["Insurance"],
                "StartDate": ["2024-01-03 09:00:00"],
                "EndDate": ["2024-01-03 10:00:00"],
                "AppointmentReason1": ["Consultation"],
                "Provider": ["doctor who"],
            }
        )
        sheet_df = pd.DataFrame(
            {
                "ID": ["1", "999"],
                "ConfirmationStatus": ["Old", "Completed"],
                "CreatedDate": ["2024-01-01 08:00:00", "2024-01-04 08:00:00"],
                "LastModifiedDate": ["2024-01-02 08:00:00", "2024-01-05 08:00:00"],
                "ServiceLocationName": ["HQ", "Branch"],
                "PatientID": ["10", "11"],
                "PatientFullName": ["John Doe", "Jane Roe"],
                "PatientCaseID": ["500", "501"],
                "PatientCaseName": ["", ""],
                "PatientCasePayerScenario": ["Insurance", "Insurance"],
                "StartDate": ["2024-01-03 09:00:00", "2024-06-06 11:00:00"],
                "EndDate": ["2024-01-03 10:00:00", "2024-06-06 12:00:00"],
                "AppointmentReason1": ["Consultation", "Follow Up"],
                "Provider": ["Doctor Who", "Doctor Two"],
                "Service": ["", "Legacy"],
                "Done": [0, 1],
                "StartOnlyDate": ["2024-01-03", "2024-06-06"],
                "WeekDate": ["12/29-01/04/24", "06/05-06/11/24"],
                "MonthDate": ["2024-01-01", "2024-06-01"],
                "Month": [1, 6],
                "Week": [1, 23],
                "Time": [0.0, 1.0],
                "CreationWeekDate": ["12/29-01/04/24", "05/31-06/06/24"],
                "CaseNameID": ["100", "200"],
            }
        )

        with patch.object(
            self.pipeline,
            "_get_extraction_bounds",
            return_value=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")),
        ):
            result = self.pipeline.process_appointments(sheet_df)

        self.assertEqual(set(result["ID"]), {"1"})

    def test_process_appointments_marks_deleted_only_inside_extraction_window(self):
        self.pipeline.appointment_df = pd.DataFrame(
            {
                "ID": ["1"],
                "ConfirmationStatus": ["Confirmed"],
                "CreatedDate": ["2024-01-01 08:00:00"],
                "LastModifiedDate": ["2024-01-02 08:00:00"],
                "ServiceLocationName": ["HQ"],
                "PatientID": ["10"],
                "PatientFullName": ["john doe"],
                "PatientCaseID": ["500"],
                "PatientCaseName": [None],
                "PatientCasePayerScenario": ["Insurance"],
                "StartDate": ["2024-01-03 09:00:00"],
                "EndDate": ["2024-01-03 10:00:00"],
                "AppointmentReason1": ["Consultation"],
                "Provider": ["doctor who"],
            }
        )
        sheet_df = pd.DataFrame(
            {
                "ID": ["2", "999"],
                "ConfirmationStatus": ["Completed", "Completed"],
                "CreatedDate": ["2024-01-01 08:00:00", "2024-01-04 08:00:00"],
                "LastModifiedDate": ["2024-01-02 08:00:00", "2024-01-05 08:00:00"],
                "ServiceLocationName": ["HQ", "Branch"],
                "PatientID": ["11", "12"],
                "PatientFullName": ["Jane Roe", "Other Roe"],
                "PatientCaseID": ["501", "502"],
                "PatientCaseName": ["", ""],
                "PatientCasePayerScenario": ["Self", "Insurance"],
                "StartDate": ["2024-01-10 09:00:00", "2024-06-06 11:00:00"],
                "EndDate": ["2024-01-10 10:00:00", "2024-06-06 12:00:00"],
                "AppointmentReason1": ["Follow Up", "Follow Up"],
                "Provider": ["Doctor Two", "Doctor Three"],
                "Service": ["Legacy", "Legacy"],
                "Done": [1, 1],
                "StartOnlyDate": ["2024-01-10", "2024-06-06"],
                "WeekDate": ["01/05-01/11/24", "06/05-06/11/24"],
                "MonthDate": ["2024-01-01", "2024-06-01"],
                "Month": [1, 6],
                "Week": [2, 23],
                "Time": [1.0, 1.0],
                "CreationWeekDate": ["12/29-01/04/24", "05/31-06/06/24"],
                "CaseNameID": ["999", "200"],
            }
        )

        with patch.object(
            self.pipeline,
            "_get_extraction_bounds",
            return_value=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")),
        ):
            result = self.pipeline.process_appointments(sheet_df).sort_values("ID").reset_index(drop=True)

        self.assertEqual(result["ID"].tolist(), ["1", "2"])
        self.assertEqual(result.loc[1, "ConfirmationStatus"], "Deleted")

    def test_build_delta_appointments_returns_only_new_and_changed_rows(self):
        processed_df = pd.DataFrame(
            [
                {
                    "ID": "1",
                    "ConfirmationStatus": "Confirmed",
                    "CreatedDate": "03/01/2026 10:00:00",
                    "LastModifiedDate": "03/02/2026 10:00:00",
                    "ServiceLocationName": "HQ",
                    "PatientID": "10",
                    "PatientFullName": "John Doe",
                    "PatientCaseID": "500",
                    "PatientCaseName": "Aetna",
                    "PatientCasePayerScenario": "Insurance",
                    "StartDate": "03/10/2026 09:00:00",
                    "EndDate": "03/10/2026 09:45:00",
                    "AppointmentReason1": "Consultation",
                    "Provider": "Doctor Who",
                    "Service": "Consult",
                    "Done": 0,
                    "StartOnlyDate": "03/10/2026",
                    "WeekDate": "03/06-03/12/26",
                    "MonthDate": "2026-03-01",
                    "Month": 3,
                    "Week": 11,
                    "Time": 0.75,
                    "CreationWeekDate": "02/27-03/05/26",
                    "CaseNameID": "100",
                },
                {
                    "ID": "2",
                    "ConfirmationStatus": "Deleted",
                    "CreatedDate": "03/01/2026 10:00:00",
                    "LastModifiedDate": "03/02/2026 10:00:00",
                    "ServiceLocationName": "HQ",
                    "PatientID": "11",
                    "PatientFullName": "Jane Roe",
                    "PatientCaseID": "501",
                    "PatientCaseName": "Selfpay",
                    "PatientCasePayerScenario": "Self",
                    "StartDate": "03/11/2026 09:00:00",
                    "EndDate": "03/11/2026 09:45:00",
                    "AppointmentReason1": "Follow Up",
                    "Provider": "Doctor Two",
                    "Service": "Deleted Service",
                    "Done": 0,
                    "StartOnlyDate": "03/11/2026",
                    "WeekDate": "03/06-03/12/26",
                    "MonthDate": "2026-03-01",
                    "Month": 3,
                    "Week": 11,
                    "Time": 0.75,
                    "CreationWeekDate": "02/27-03/05/26",
                    "CaseNameID": "999",
                },
            ],
            columns=BQ_COLUMNS,
        )
        sheet_df = processed_df.copy()
        sheet_df.loc[sheet_df["ID"] == "2", "ConfirmationStatus"] = "Completed"

        result = self.pipeline.build_delta_appointments(processed_df, sheet_df)

        self.assertEqual(result["ID"].tolist(), ["2"])

    def test_build_delta_appointments_returns_empty_when_no_changes(self):
        processed_df = pd.DataFrame(
            [
                {
                    "ID": "1",
                    "ConfirmationStatus": "Confirmed",
                    "CreatedDate": "03/01/2026 10:00:00",
                    "LastModifiedDate": "03/02/2026 10:00:00",
                    "ServiceLocationName": "HQ",
                    "PatientID": "10",
                    "PatientFullName": "John Doe",
                    "PatientCaseID": "500",
                    "PatientCaseName": "Aetna",
                    "PatientCasePayerScenario": "Insurance",
                    "StartDate": "03/10/2026 09:00:00",
                    "EndDate": "03/10/2026 09:45:00",
                    "AppointmentReason1": "Consultation",
                    "Provider": "Doctor Who",
                    "Service": "Consult",
                    "Done": 0,
                    "StartOnlyDate": "03/10/2026",
                    "WeekDate": "03/06-03/12/26",
                    "MonthDate": "2026-03-01",
                    "Month": 3,
                    "Week": 11,
                    "Time": 0.75,
                    "CreationWeekDate": "02/27-03/05/26",
                    "CaseNameID": "100",
                }
            ],
            columns=BQ_COLUMNS,
        )

        result = self.pipeline.build_delta_appointments(processed_df, processed_df.copy())

        self.assertTrue(result.empty)

    @patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "C:\\fake\\creds.json"}, clear=False)
    @patch("main.Path.exists", return_value=True)
    @patch("main.AppointmentPipeline._build_service_account_credentials")
    def test_authenticate_prefers_service_account(
        self, mock_build_service_account, _mock_exists
    ):
        sentinel_credentials = object()
        mock_build_service_account.return_value = sentinel_credentials

        result = self.pipeline.authenticate()

        self.assertIs(result, sentinel_credentials)
        mock_build_service_account.assert_called_once_with("C:\\fake\\creds.json")

    @patch.dict(
        os.environ,
        {
            "CLIENT_ID": "client-id",
            "CLIENT_SECRET": "client-secret",
            "REFRESH_TOKEN": "refresh-token",
        },
        clear=True,
    )
    @patch("google.oauth2.credentials.Credentials.refresh")
    def test_authenticate_uses_oauth_when_service_account_missing(self, mock_refresh):
        credentials = self.pipeline.authenticate()
        self.assertEqual(credentials.client_id, "client-id")
        self.assertEqual(credentials.refresh_token, "refresh-token")
        mock_refresh.assert_called_once()

    @patch.dict(
        os.environ,
        {
            "CLIENT_ID": "client-id",
            "CLIENT_SECRET": "client-secret",
            "REFRESH_TOKEN": "refresh-token",
        },
        clear=True,
    )
    @patch("google.oauth2.credentials.Credentials.refresh", side_effect=RefreshError("invalid_scope"))
    def test_authenticate_reports_invalid_oauth_scope(self, _mock_refresh):
        with self.assertRaises(RuntimeError) as exc:
            self.pipeline.authenticate()

        self.assertIn("Regenerate the token", str(exc.exception))

    @patch("main.bigquery.Client")
    def test_init_bigquery_client_reuses_shared_credentials(self, mock_client_cls):
        credentials = object()
        self.pipeline.init_bigquery_client(credentials)

        mock_client_cls.assert_called_once_with(
            project="weekly-revenue-integration",
            credentials=credentials,
            location="us",
        )


class TransformationTests(unittest.TestCase):
    def test_assign_patient_case_name_handles_mixed_id_types(self):
        df = pd.DataFrame(
            {
                "PatientID": ["1", "2", "x"],
                "PatientCaseName": [None, None, None],
                "ID": [101, 102, 103],
                "PatientCasePayerScenario": ["Other", "Other", "Self"],
            }
        )
        patient_df = pd.DataFrame(
            {"ID": [1, 2], "PrimaryInsurancePolicyCompanyName": ["Ins1", "Ins2"]}
        )

        result = assign_patient_case_name(df, patient_df)

        self.assertEqual(result.loc[0, "PatientCaseName"], "Ins1")
        self.assertEqual(result.loc[1, "PatientCaseName"], "Ins2")
        self.assertEqual(result.loc[2, "PatientCaseName"], "Selfpay")

    def test_apply_column_service_preserves_existing_service_when_mapping_missing(self):
        df = pd.DataFrame(
            {"AppointmentReason1": ["Known", "Unknown"], "Service": ["Original", "Keep Me"]}
        )
        master_df = pd.DataFrame({"AppointmentReason1": ["Known"], "Service": ["Mapped"]})

        result = apply_column_service(df, master_df)

        self.assertEqual(result["Service"].tolist(), ["Mapped", "Keep Me"])

    def test_normalize_identifier_columns_removes_decimal_suffix(self):
        df = pd.DataFrame(
            {
                "PatientID": [15011.0, "21369.0", None],
                "CaseNameID": [100.0, "200", ""],
            }
        )

        result = normalize_identifier_columns(df, ["PatientID", "CaseNameID"])

        self.assertEqual(result["PatientID"].tolist(), ["15011", "21369", None])
        self.assertEqual(result["CaseNameID"].tolist(), ["100", "200", None])

    # --- WeekDate (Friday-to-Thursday) tests ---

    def test_week_date_friday_to_thursday_basic(self):
        """A Monday should fall within the Fri-Thu range that started the previous Friday."""
        # Monday 2026-03-16 → week is Fri 03/13 to Thu 03/19
        df = pd.DataFrame({"StartDate": ["2026-03-16 10:00:00"]})
        result = apply_column_week_date(df)
        self.assertEqual(result.loc[0, "WeekDate"], "03/13-03/19/26")

    def test_week_date_on_friday(self):
        """A Friday should start its own week."""
        # Friday 2026-03-13
        df = pd.DataFrame({"StartDate": ["2026-03-13 10:00:00"]})
        result = apply_column_week_date(df)
        self.assertEqual(result.loc[0, "WeekDate"], "03/13-03/19/26")

    def test_week_date_on_thursday(self):
        """A Thursday should be the last day of its week."""
        # Thursday 2026-03-19
        df = pd.DataFrame({"StartDate": ["2026-03-19 10:00:00"]})
        result = apply_column_week_date(df)
        self.assertEqual(result.loc[0, "WeekDate"], "03/13-03/19/26")

    def test_week_date_cross_month(self):
        """Week that crosses from March into April."""
        # Monday 2026-03-30 → week is Fri 03/27 to Thu 04/02
        df = pd.DataFrame({"StartDate": ["2026-03-30 10:00:00"]})
        result = apply_column_week_date(df)
        self.assertEqual(result.loc[0, "WeekDate"], "03/27-04/02/26")

    def test_week_date_cross_year(self):
        """Week that crosses from December into January."""
        # Monday 2025-12-29 → week is Fri 12/26 to Thu 01/01
        df = pd.DataFrame({"StartDate": ["2025-12-29 10:00:00"]})
        result = apply_column_week_date(df)
        self.assertEqual(result.loc[0, "WeekDate"], "12/26-01/01/26")

    def test_week_date_null_start_date(self):
        """Null StartDate should produce null WeekDate."""
        df = pd.DataFrame({"StartDate": [None]})
        result = apply_column_week_date(df)
        self.assertIsNone(result.loc[0, "WeekDate"])

    # --- Done tests ---

    def test_done_check_out_maps_to_1_without_master(self):
        """Check-Out should map to Done=1 using only the local catalog."""
        df = pd.DataFrame({"ConfirmationStatus": ["Check-Out", "Scheduled"]})
        result = apply_column_done(df, master_df=None)
        self.assertEqual(result["Done"].tolist(), [1, 0])

    def test_done_unknown_status_maps_to_0(self):
        """An unknown status should default to Done=0."""
        df = pd.DataFrame({"ConfirmationStatus": ["SomeNewStatus"]})
        result = apply_column_done(df, master_df=None)
        self.assertEqual(result.loc[0, "Done"], 0)

    def test_done_master_overrides_local(self):
        """Master sheet mappings should override the local catalog."""
        master_df = pd.DataFrame(
            {"ConfirmationStatus": ["Check-Out", "Completed"], "Done": [1, 1]}
        )
        df = pd.DataFrame({"ConfirmationStatus": ["Completed", "Cancelled"]})
        result = apply_column_done(df, master_df=master_df)
        self.assertEqual(result["Done"].tolist(), [1, 0])

    # --- StartOnlyDate ---

    def test_start_only_date_is_real_date(self):
        """StartOnlyDate should be a Timestamp with time=00:00:00."""
        df = pd.DataFrame({"StartDate": ["2026-03-18 14:30:00"]})
        result = format_column_date(df)
        val = result.loc[0, "StartOnlyDate"]
        self.assertIsInstance(val, pd.Timestamp)
        self.assertEqual(val.hour, 0)
        self.assertEqual(val.minute, 0)
        self.assertEqual(val.date(), pd.Timestamp("2026-03-18").date())

    # --- CreationWeekDate ---

    def test_creation_week_date_uses_created_date(self):
        """CreationWeekDate should derive from CreatedDate, not StartDate."""
        df = pd.DataFrame({
            "StartDate": ["2026-03-18 10:00:00"],
            "CreatedDate": ["2026-03-10 10:00:00"],  # Mon → Fri 03/06 – Thu 03/12
        })
        result = apply_column_creation_week_date(df)
        self.assertEqual(result.loc[0, "CreationWeekDate"], "03/06-03/12/26")

    # --- Time ---

    def test_time_negative_clamped_to_zero(self):
        """When EndDate < StartDate, Time should be clamped to 0."""
        df = pd.DataFrame({
            "StartDate": ["2026-03-18 10:00:00"],
            "EndDate": ["2026-03-18 09:00:00"],  # -1 hour
            "Done": [1],
        })
        result = apply_time_column(df)
        self.assertEqual(result.loc[0, "Time"], 0.0)

    # --- trim_string_columns ---

    def test_trim_string_columns(self):
        """Leading/trailing spaces should be stripped."""
        df = pd.DataFrame({
            "ConfirmationStatus": ["  Check-Out  "],
            "ServiceLocationName": ["  Clinic A "],
        })
        result = trim_string_columns(df)
        self.assertEqual(result.loc[0, "ConfirmationStatus"], "Check-Out")
        self.assertEqual(result.loc[0, "ServiceLocationName"], "Clinic A")

    # --- validate_output_schema ---

    def test_validate_output_schema_catches_violations(self):
        """Validator should detect Done outside {0,1} and negative Time."""
        df = pd.DataFrame({
            "Done": [2],
            "Month": [13],
            "Time": [-1.0],
            "WeekDate": ["bad-format"],
            "StartDate": ["2026-03-18 10:00:00"],
        })
        warnings = validate_output_schema(df)
        self.assertTrue(len(warnings) >= 3)  # Done, Month, WeekDate at minimum


class BigQueryOpsTests(unittest.TestCase):
    def test_filter_recent_appointments_requires_last_modified_date(self):
        with self.assertRaises(KeyError):
            filter_recent_appointments(pd.DataFrame({"ID": ["1"]}))

    def test_load_to_bigquery_rejects_invalid_identifier(self):
        with self.assertRaises(ValueError):
            load_to_bigquery("project", "bad-dataset", "table", pd.DataFrame(), "replace", credentials=object())

    @patch("bigquery_ops.pandas_gbq.to_gbq")
    def test_load_to_bigquery_passes_explicit_credentials(self, mock_to_gbq):
        credentials = object()
        load_to_bigquery(
            "project",
            "dataset_1",
            "table_1",
            pd.DataFrame({"ID": ["1"]}),
            "replace",
            credentials=credentials,
        )

        self.assertEqual(mock_to_gbq.call_args.kwargs["credentials"], credentials)

    def test_validate_bigquery_destination_reports_missing_table(self):
        client = MagicMock()
        client.get_dataset.return_value = object()
        client.get_table.side_effect = NotFound("missing")

        with self.assertRaises(FileNotFoundError):
            validate_bigquery_destination(client, "project", "dataset", "table")


class SheetsClientTests(unittest.TestCase):
    def test_ensure_sheet_size_resizes_only_when_needed(self):
        sheet = MagicMock()
        sheet.row_count = 3000
        sheet.col_count = 25

        client = SheetsClient(gc=MagicMock())
        client._ensure_sheet_size(sheet, data_rows=4520, data_cols=24, start_row=1, start_col=1, include_header=True)

        sheet.resize.assert_called_once_with(rows=4521, cols=24)

    def test_import_async_starts_second_chunk_after_header_and_first_chunk(self):
        client = SheetsClient(gc=MagicMock())
        sheet = MagicMock()
        df = pd.DataFrame({"A": list(range(6))})

        with patch.object(client, "_import_fragment") as mock_import_fragment:
            client._import_async(
                sheet=sheet,
                df=df,
                chunk_size=3,
                max_retries=1,
                delay=0,
                row=1,
                col=1,
                include_header=True,
                resize=False,
            )

        first_call = mock_import_fragment.call_args_list[0]
        second_call = mock_import_fragment.call_args_list[1]

        self.assertEqual(first_call.args[2], 1)
        self.assertTrue(first_call.args[3])
        self.assertEqual(second_call.args[2], 5)
        self.assertFalse(second_call.args[3])

    def test_upsert_dataframe_updates_existing_rows_and_appends_new_rows(self):
        client = SheetsClient(gc=MagicMock())
        spreadsheet = MagicMock()
        sheet = MagicMock()
        sheet.row_count = 10
        sheet.col_count = 24
        spreadsheet.worksheet.return_value = sheet
        sheet.get_all_values.return_value = [
            ["ID", "ConfirmationStatus"],
            ["1", "Old"],
        ]

        df = pd.DataFrame(
            [
                {"ID": "1", "ConfirmationStatus": "Updated"},
                {"ID": "2", "ConfirmationStatus": "New"},
            ]
        )

        with patch.object(client, "_batch_update_rows") as mock_batch_update, patch.object(
            client, "_append_rows"
        ) as mock_append_rows:
            result = client.upsert_dataframe(spreadsheet, df, "Appointment")

        self.assertTrue(result)
        mock_batch_update.assert_called_once()
        updates_arg = mock_batch_update.call_args.args[1]
        self.assertEqual(len(updates_arg), 1)
        self.assertEqual(updates_arg[0][0], 2)                # row number
        self.assertEqual(updates_arg[0][1], ["1", "Updated"]) # values
        mock_append_rows.assert_called_once_with(sheet, [["2", "New"]], 3, 30)

    def test_validate_bigquery_destination_reports_forbidden_table(self):
        client = MagicMock()
        client.get_dataset.return_value = object()
        client.get_table.side_effect = Forbidden("forbidden")

        with self.assertRaises(PermissionError):
            validate_bigquery_destination(client, "project", "dataset", "table")


if __name__ == "__main__":
    unittest.main()
