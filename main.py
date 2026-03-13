"""
Main orchestration script for Appointment extraction from Tebra.
Designed for Google Cloud Run deployment.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

from config_loader import get_config
from tebra_client import TebraClient
from bigquery_ops import (
    load_to_bigquery, convert_columns, filter_recent_appointments, merge_appointments
)
from sheets_ops import SheetsClient, get_sheet_data
from transformations import (
    generate_date_range, find_appointments_not_in_tebra, apply_all_transformations,
    mark_deleted_appointments, assign_patient_case_name, assign_case_name_id,
    apply_column_done, apply_time_column, format_datetime_columns
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AppointmentPipeline:
    def __init__(self, config):
        self.config = config
        self.tebra_client = None
        self.sheets_client = None
        self.appointment_df = None
        self.master_df = None
        self.patient_df = None
        self.insurance_df = None
    
    def authenticate(self, use_service_account: bool = True):
        from google.oauth2 import service_account
        from google.auth import default
        
        if use_service_account:
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path and Path(creds_path).exists():
                creds = service_account.Credentials.from_service_account_file(creds_path)
            else:
                creds, _ = default()
        else:
            import gspread
            creds, _ = default()
        
        import gspread
        gc = gspread.auth.authorize(creds)
        self.sheets_client = SheetsClient(gc)
        return gc
    
    def init_tebra_client(self, client):
        tebra_config = self.config.tebra
        request_header = {
            'User': tebra_config['user'],
            'Password': tebra_config['password'],
            'CustomerKey': tebra_config['customer_key']
        }
        self.tebra_client = TebraClient(client, request_header)
    
    def load_master_data(self, gc):
        logger.info("Loading master data...")
        
        sheets_config = self.config.google_sheets
        
        self.master_df = get_sheet_data(sheets_config['sheet_key'], 'Master', gc)
        self.master_df = self.master_df[["Reason", "Service", "Status", "Active"]].drop_duplicates()
        self.master_df.rename(columns={
            "Status": "ConfirmationStatus",
            "Active": "Done",
            "Reason": "AppointmentReason1"
        }, inplace=True)
        
        self.patient_df = get_sheet_data(
            sheets_config['patient_sheet_key'], 'PatientsInformation', gc
        )
        self.patient_df = self.patient_df[['ID', 'PrimaryInsurancePolicyCompanyName']].replace("", None)
        
        self.insurance_df = get_sheet_data(
            sheets_config['insurance_sheet_key'], 'Insurances_Names', gc
        )
        self.insurance_df.replace("", None, inplace=True)
        
        logger.info(f"Master: {len(self.master_df)}, Patients: {len(self.patient_df)}, Insurances: {len(self.insurance_df)}")
    
    def load_sheet_appointments(self, gc):
        logger.info("Loading sheet appointments...")
        sheets_config = self.config.google_sheets
        return get_sheet_data(sheets_config['sheet_key'], 'Appointment', gc)
    
    def extract_tebra_appointments(self, parallel: bool = True):
        logger.info("Extracting appointments from Tebra...")
        
        pipeline_config = self.config.pipeline
        
        dates = generate_date_range(
            months_back=pipeline_config['months_back'],
            months_forward=pipeline_config['months_forward'],
            freq_days=pipeline_config['fetch_interval_days']
        )
        
        if parallel:
            self.appointment_df = self.tebra_client.fetch_appointments_parallel(
                dates, pipeline_config['parallel_workers']
            )
        else:
            self.appointment_df = self.tebra_client.fetch_appointments_sequential(dates)
        
        self.appointment_df.dropna(subset=['ID'], inplace=True)
        return self.appointment_df
    
    def process_appointments(self, sheet_appointments_df):
        logger.info("Processing appointments...")
        
        df = self.appointment_df.copy()
        df = df.replace({r"´": "", None: ""}, regex=True)
        
        df = apply_all_transformations(df, self.master_df)
        
        deleted_ids = find_appointments_not_in_tebra(df, sheet_appointments_df)
        df = df.replace("", None).dropna(subset=['ID'])
        
        df = df.merge(
            sheet_appointments_df, on="ID", how="outer", suffixes=("_tebra", "_sheet")
        )
        
        columns_appointment_sheet = sheet_appointments_df.columns.tolist()
        import_df = df[columns_appointment_sheet].copy()
        
        datetime_cols = ['StartDate', 'EndDate', 'CreatedDate', 'LastModifiedDate']
        for col in datetime_cols:
            import_df[col] = pd.to_datetime(import_df[col], format='mixed', errors='coerce')
        
        import_df = apply_column_done(import_df, self.master_df)
        import_df = apply_time_column(import_df)
        import_df = mark_deleted_appointments(import_df, deleted_ids)
        import_df = assign_patient_case_name(import_df, self.patient_df)
        import_df = assign_case_name_id(import_df, self.insurance_df)
        import_df = format_datetime_columns(import_df)
        
        return import_df
    
    def upload_to_bigquery(self, df: pd.DataFrame):
        logger.info("Uploading to BigQuery...")
        
        bq_config = self.config.bigquery
        exec_config = self.config.execution
        
        df_filtered = filter_recent_appointments(df, exec_config['days_back_filter'])
        logger.info(f"Filtered {len(df_filtered)} recent records")
        
        df_converted = convert_columns(df_filtered)
        
        load_to_bigquery(
            bq_config['project_id'],
            bq_config['dataset_id'],
            bq_config['source_table'],
            df_converted,
            "replace"
        )
        
        merge_appointments(
            bq_config['project_id'],
            bq_config['dataset_id'],
            bq_config['source_table'],
            bq_config['target_table']
        )
    
    def upload_to_sheets(self, df: pd.DataFrame, gc):
        logger.info("Uploading to Google Sheets...")
        
        sheets_config = self.config.google_sheets
        pipeline_config = self.config.pipeline
        
        spreadsheet = self.sheets_client.open_spreadsheet(sheets_config['sheet_key'])
        
        self.sheets_client.import_dataframe(
            spreadsheet, df, 'Appointment',
            async_mode=pipeline_config['async_upload'],
            chunk_size=pipeline_config['chunk_size']
        )
    
    def run(self):
        try:
            config_errors = self.config.validate()
            if config_errors:
                for error in config_errors:
                    logger.error(error)
                sys.exit(1)
            
            gc = self.authenticate()
            
            from zeep import Client
            tebra_config = self.config.tebra
            client = Client(tebra_config['wsdl'])
            self.init_tebra_client(client)
            
            self.load_master_data(gc)
            sheet_appointments = self.load_sheet_appointments(gc)
            
            self.extract_tebra_appointments(parallel=True)
            
            processed_df = self.process_appointments(sheet_appointments)
            
            self.upload_to_bigquery(processed_df)
            self.upload_to_sheets(processed_df, gc)
            
            logger.info("Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            return False


def main():
    config = get_config()
    
    pipeline = AppointmentPipeline(config)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
