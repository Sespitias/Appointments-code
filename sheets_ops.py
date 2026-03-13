import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import Optional


class SheetsClient:
    def __init__(self, gc):
        self.gc = gc
    
    def open_spreadsheet(self, key: str, retries: int = 3, delay: int = 30):
        for attempt in range(retries):
            try:
                return self.gc.open_by_key(key)
            except Exception as e:
                if attempt < retries - 1:
                    print(f"Retry {attempt + 1}/{retries} for spreadsheet {key}")
                    time.sleep(delay)
                else:
                    raise
    
    def get_worksheet(self, spreadsheet, sheet_id: Optional[int] = None, sheet_name: Optional[str] = None):
        if sheet_id is not None:
            return spreadsheet.get_worksheet_by_id(sheet_id)
        return spreadsheet.worksheet(sheet_name)
    
    def import_dataframe(
        self,
        spreadsheet,
        df: pd.DataFrame,
        sheet_identifier,
        async_mode: bool = False,
        chunk_size: int = 500,
        max_retries: int = 3,
        delay: int = 60,
        row: int = 1,
        col: int = 1,
        include_header: bool = True,
        resize: bool = True,
        clear_sheet: bool = True
    ) -> bool:
        try:
            sheet = self._resolve_worksheet(spreadsheet, sheet_identifier)
        except Exception as e:
            print(f"Worksheet not found: {e}")
            return False
        
        if clear_sheet:
            sheet.clear()
        
        if async_mode and len(df) > chunk_size:
            return self._import_async(sheet, df, chunk_size, max_retries, delay, row, col, include_header, resize)
        return self._import_sync(sheet, df, max_retries, delay, row, col, include_header, resize)
    
    def _resolve_worksheet(self, spreadsheet, identifier):
        try:
            return spreadsheet.get_worksheet_by_id(int(identifier))
        except (ValueError, gspread.WorksheetNotFound):
            return spreadsheet.worksheet(identifier)
    
    def _import_sync(self, sheet, df, max_retries, delay, row, col, include_header, resize):
        for attempt in range(max_retries):
            try:
                set_with_dataframe(
                    sheet, df, row=row, col=col,
                    include_column_header=include_header,
                    resize=resize
                )
                print(f"Imported {len(df)} rows successfully")
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Retry {attempt + 1}: {e}")
                    time.sleep(delay)
                else:
                    raise
        return False
    
    def _import_async(self, sheet, df, chunk_size, max_retries, delay, row, col, include_header, resize):
        total_rows = len(df)
        tasks = []
        
        with ThreadPoolExecutor() as executor:
            for start in range(0, total_rows, chunk_size):
                end = min(start + chunk_size, total_rows)
                fragment = df.iloc[start:end]
                start_row = row + start + (1 if include_header and start == 0 else 0)
                header_flag = include_header and start == 0
                tasks.append(executor.submit(
                    self._import_fragment, sheet, fragment, start_row, header_flag, 
                    max_retries, delay, resize
                ))
            
            for future in as_completed(tasks):
                try:
                    future.result()
                except Exception as e:
                    print(f"Chunk error: {e}")
                    return False
        return True
    
    def _import_fragment(self, sheet, df, start_row, header_flag, max_retries, delay, resize):
        for attempt in range(max_retries):
            try:
                set_with_dataframe(
                    sheet, df, row=start_row,
                    include_column_header=header_flag,
                    resize=resize
                )
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    raise
    
    def update_columns(
        self,
        spreadsheet,
        df: pd.DataFrame,
        sheet_identifier,
        include_columns: list = None,
        exclude_columns: list = None,
        max_retries: int = 3,
        retry_delay: int = 30
    ) -> bool:
        try:
            worksheet = self._resolve_worksheet(spreadsheet, sheet_identifier)
        except Exception as e:
            print(f"Worksheet not found: {e}")
            return False
        
        google_cols = worksheet.row_values(1)
        df_cols = df.columns.tolist()
        
        if include_columns:
            df_cols = [c for c in df_cols if c in include_columns]
        if exclude_columns:
            df_cols = [c for c in df_cols if c not in exclude_columns]
        
        for col_name in df_cols:
            if col_name not in google_cols:
                continue
            col_index = google_cols.index(col_name) + 1
            col_data = df[col_name].values.reshape(-1, 1)
            
            for attempt in range(max_retries):
                try:
                    worksheet.update(
                        f"{gspread.utils.rowcol_to_a1(2, col_index)}:{gspread.utils.rowcol_to_a1(1 + len(df), col_index)}",
                        col_data.tolist()
                    )
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        print(f"Failed to update {col_name}: {e}")
                        return False
        return True


def get_sheet_data(spreadsheet_key: str, sheet_name: str, gc) -> pd.DataFrame:
    from gspread.exceptions import SpreadsheetNotFound, APIError
    
    for attempt in range(3):
        try:
            spreadsheet = gc.open_by_key(spreadsheet_key)
            worksheet = spreadsheet.worksheet(sheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
            return df
        except (SpreadsheetNotFound, APIError) as e:
            if attempt < 2:
                time.sleep(30)
            else:
                raise
    return pd.DataFrame()
