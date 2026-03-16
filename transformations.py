import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re


def apply_column_service(df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    service_map = dict(zip(master_df['AppointmentReason1'], master_df['Service']))
    existing_service = df['Service'] if 'Service' in df.columns else pd.Series(index=df.index, dtype='object')
    mapped_service = df['AppointmentReason1'].map(service_map)
    df['Service'] = mapped_service.fillna(existing_service)
    return df


def apply_column_done(df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    done_map = dict(zip(master_df['ConfirmationStatus'], master_df['Done']))
    done_values = pd.to_numeric(df['ConfirmationStatus'].map(done_map), errors='coerce')
    df['Done'] = done_values.fillna(0).astype(int)
    return df


def format_column_date(df: pd.DataFrame) -> pd.DataFrame:
    date_cols = ['StartDate', 'EndDate', 'CreatedDate']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='mixed', errors='coerce')

    if 'StartDate' in df.columns:
        df['StartOnlyDate'] = pd.to_datetime(df['StartDate'], errors='coerce').dt.date
    return df


def apply_column_month(df: pd.DataFrame) -> pd.DataFrame:
    if 'StartDate' in df.columns:
        df['Month'] = pd.to_datetime(df['StartDate'], errors='coerce').dt.month
    return df


def apply_column_week(df: pd.DataFrame) -> pd.DataFrame:
    if 'StartOnlyDate' in df.columns:
        df['Week'] = pd.to_datetime(df['StartOnlyDate'], errors='coerce').dt.isocalendar().week
    return df


def apply_column_week_date(df: pd.DataFrame) -> pd.DataFrame:
    if 'StartOnlyDate' in df.columns:
        start_dates = pd.to_datetime(df['StartOnlyDate'], errors='coerce')
        df['WeekDate'] = start_dates.dt.isocalendar().week.astype(str) + '-' + start_dates.dt.year.astype(str)

        df['CreationWeekDate'] = start_dates.dt.isocalendar().week.astype(str) + '-' + start_dates.dt.year.astype(str)
    return df


def apply_column_month_date(df: pd.DataFrame) -> pd.DataFrame:
    if 'StartDate' in df.columns:
        df['MonthDate'] = pd.to_datetime(df['StartDate'], errors='coerce').dt.strftime('%B %Y')
    return df


def apply_time_column(df: pd.DataFrame) -> pd.DataFrame:
    if 'StartDate' in df.columns and 'EndDate' in df.columns:
        start = pd.to_datetime(df['StartDate'], errors='coerce')
        end = pd.to_datetime(df['EndDate'], errors='coerce')
        duration_hours = (end - start).dt.total_seconds() / 3600
        df['Time'] = duration_hours * df['Done'].fillna(0)
    return df


def capitalize_string_columns(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    if columns is None:
        columns = ['PatientFullName', 'PatientCaseName', 'AppointmentReason1',
                   'Provider', 'Service', 'PatientCasePayerScenario']
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.title()
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({r"´": "", np.nan: None}, regex=True)
    return df


def find_appointments_not_in_tebra(tebra_df: pd.DataFrame, sheet_df: pd.DataFrame) -> list:
    tebra_ids = set(tebra_df['ID'].dropna().astype(str))
    sheet_ids = set(sheet_df['ID'].dropna().astype(str))
    return list(sheet_ids - tebra_ids)


def mark_deleted_appointments(df: pd.DataFrame, deleted_ids: list) -> pd.DataFrame:
    if 'ID' not in df.columns:
        return df
    df = df.copy()
    df.loc[df['ID'].isin(deleted_ids), 'ConfirmationStatus'] = 'Deleted'
    df.loc[df['ConfirmationStatus'] == 'Deleted', 'Done'] = 0
    df.loc[df['ConfirmationStatus'] == 'Deleted', 'Time'] = 0
    return df


def normalize_identifier_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
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


def assign_patient_case_name(df: pd.DataFrame, patient_df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    patient_df = patient_df.copy()
    if 'PatientID' in df.columns:
        df['_PatientID_numeric'] = pd.to_numeric(df['PatientID'], errors='coerce')
    if 'ID' in patient_df.columns:
        patient_df['_ID_numeric'] = pd.to_numeric(patient_df['ID'], errors='coerce')
    df = df.merge(
        patient_df[['ID', '_ID_numeric', 'PrimaryInsurancePolicyCompanyName']],
        left_on='_PatientID_numeric',
        right_on='_ID_numeric',
        how='left',
        suffixes=('', '_patient')
    )

    df['PatientCaseName'] = df['PatientCaseName'].replace('', pd.NA).fillna(
        df['PrimaryInsurancePolicyCompanyName']
    ).replace('-', 'Selfpay')

    df.drop(
        columns=['ID_patient', 'PrimaryInsurancePolicyCompanyName', '_PatientID_numeric', '_ID_numeric'],
        inplace=True,
        errors='ignore'
    )

    df.loc[df['PatientCasePayerScenario'].str.contains('Self', na=False), 'PatientCaseName'] = 'Selfpay'
    return df


def assign_case_name_id(df: pd.DataFrame, insurance_df: pd.DataFrame) -> pd.DataFrame:
    insurance_map = (
        insurance_df[['CaseName', 'InsuranceID']]
        .dropna(subset=['CaseName'])
        .drop_duplicates(subset=['CaseName'], keep='last')
        .set_index('CaseName')['InsuranceID']
    )
    df = df.copy()
    df['CaseNameID'] = df['PatientCaseName'].map(insurance_map)
    return df


def format_datetime_columns(df: pd.DataFrame, columns: list = None, fmt: str = '%m/%d/%Y %H:%M:%S') -> pd.DataFrame:
    if columns is None:
        columns = ['StartDate', 'EndDate', 'CreatedDate', 'LastModifiedDate']
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime(fmt)
    return df


def apply_all_transformations(df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    df = clean_dataframe(df)
    df = capitalize_string_columns(df)
    df = apply_column_service(df, master_df)
    df = format_column_date(df)
    df = apply_column_month(df)
    df = apply_column_week(df)
    df = apply_column_week_date(df)
    df = apply_column_month_date(df)
    df.drop_duplicates(inplace=True)
    return df


def generate_date_range(months_back: int = 1, months_forward: int = 1, freq_days: int = 8) -> list:
    from_date = datetime.now() - relativedelta(months=months_back)
    until_date = datetime.now() + relativedelta(months=months_forward)
    return pd.date_range(from_date, until_date, freq=f'{freq_days}D').strftime('%m/%d/%Y').tolist()
