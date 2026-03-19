import logging
import time
from typing import Optional

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1


logger = logging.getLogger(__name__)


class SheetsClient:
    def __init__(self, gc):
        self.gc = gc
    
    def open_spreadsheet(self, key: str, retries: int = 3, delay: int = 30):
        for attempt in range(retries):
            try:
                return self.gc.open_by_key(key)
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning("Retry %s/%s for spreadsheet %s", attempt + 1, retries, key)
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
            logger.exception("Worksheet not found: %s", e)
            return False

        self._ensure_sheet_size(sheet, len(df), len(df.columns), row, col, include_header)
        
        if clear_sheet:
            sheet.clear()
        
        if async_mode and len(df) > chunk_size:
            return self._import_async(sheet, df, chunk_size, max_retries, delay, row, col, include_header, resize)
        return self._import_sync(sheet, df, max_retries, delay, row, col, include_header, resize)

    def upsert_dataframe(
        self,
        spreadsheet,
        df: pd.DataFrame,
        sheet_identifier,
        key_column: str = "ID",
        max_retries: int = 3,
        delay: int = 30,
    ) -> bool:
        try:
            sheet = self._resolve_worksheet(spreadsheet, sheet_identifier)
        except Exception as e:
            logger.exception("Worksheet not found: %s", e)
            return False

        if df.empty:
            self._sync_header(sheet, df.columns.tolist())
            logger.info("No delta rows to upsert into Google Sheets")
            return True

        self._sync_header(sheet, df.columns.tolist())
        existing_df = self._get_existing_data_with_rows(sheet)

        if key_column not in existing_df.columns:
            existing_df = pd.DataFrame(columns=df.columns.tolist() + ["__row_num__"])

        if key_column not in df.columns:
            raise KeyError(f"Missing key column for upsert: {key_column}")

        prepared_df = self._prepare_sheet_dataframe(df)
        existing_lookup = {}
        if not existing_df.empty:
            for _, row in existing_df.iterrows():
                existing_lookup[str(row[key_column])] = int(row["__row_num__"])

        rows_to_update = []
        rows_to_append = []
        for _, row in prepared_df.iterrows():
            row_key = str(row[key_column])
            row_values = row[df.columns.tolist()].tolist()
            row_num = existing_lookup.get(row_key)
            if row_num is None:
                rows_to_append.append(row_values)
            else:
                rows_to_update.append((row_num, row_values))

        required_rows = max(sheet.row_count, 1 + len(existing_df) + len(rows_to_append))
        required_cols = max(sheet.col_count, len(df.columns))
        if required_rows != sheet.row_count or required_cols != sheet.col_count:
            sheet.resize(rows=required_rows, cols=required_cols)

        if rows_to_update:
            self._batch_update_rows(sheet, rows_to_update, max_retries, delay)

        if rows_to_append:
            self._append_rows(sheet, rows_to_append, max_retries, delay)

        logger.info(
            "Upserted %s rows into Google Sheets (%s updates, %s inserts)",
            len(prepared_df),
            len(rows_to_update),
            len(rows_to_append),
        )
        return True
    
    def _resolve_worksheet(self, spreadsheet, identifier):
        try:
            return spreadsheet.get_worksheet_by_id(int(identifier))
        except (ValueError, gspread.WorksheetNotFound):
            return spreadsheet.worksheet(identifier)

    def _sync_header(self, sheet, columns: list[str]) -> None:
        header_values = [columns]
        required_cols = max(sheet.col_count, len(columns), 1)
        if sheet.col_count != required_cols:
            sheet.resize(rows=max(sheet.row_count, 1), cols=required_cols)
        end_cell = rowcol_to_a1(1, len(columns))
        sheet.update(f"A1:{end_cell}", header_values)

    def _get_existing_data_with_rows(self, sheet) -> pd.DataFrame:
        all_values = sheet.get_all_values()
        if not all_values:
            return pd.DataFrame(columns=["__row_num__"])

        header = all_values[0]
        data = all_values[1:]
        if not data:
            return pd.DataFrame(columns=header + ["__row_num__"])

        df = pd.DataFrame(data, columns=header)
        df["__row_num__"] = pd.RangeIndex(2, 2 + len(df))
        return df

    def _prepare_sheet_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        prepared = df.copy()
        prepared = prepared.where(~prepared.isna(), "")
        return prepared.astype(str).reset_index(drop=True)

    def _batch_update_rows(
        self,
        sheet,
        updates: list[tuple[int, list[str]]],
        max_retries: int,
        delay: int,
        chunk_size: int = 200,
    ) -> None:
        """Send all row updates in batched API calls to avoid 429 quota errors.

        Builds A1-notation ranges for every row and sends them in groups of
        ``chunk_size`` via a single ``values_batch_update`` call per group.
        """
        # Build the list of {range, values} dicts for the Sheets API.
        data = [
            {
                "range": f"A{row_num}:{rowcol_to_a1(row_num, len(row_values))}",
                "values": [row_values],
            }
            for row_num, row_values in updates
        ]

        for chunk_start in range(0, len(data), chunk_size):
            chunk = data[chunk_start : chunk_start + chunk_size]
            for attempt in range(max_retries):
                try:
                    sheet.spreadsheet.values_batch_update(
                        {
                            "valueInputOption": "USER_ENTERED",
                            "data": chunk,
                        }
                    )
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            "Batch update retry %s/%s: %s", attempt + 1, max_retries, e
                        )
                        time.sleep(delay)
                    else:
                        raise
            # Brief pause between chunks to respect per-minute write quota.
            if chunk_start + chunk_size < len(data):
                time.sleep(2)

    def _update_row(self, sheet, row_num: int, row_values: list[str], max_retries: int, delay: int) -> None:
        """Single-row update (kept for backward compatibility / unit tests)."""
        range_name = f"A{row_num}:{rowcol_to_a1(row_num, len(row_values))}"
        for attempt in range(max_retries):
            try:
                sheet.update(range_name, [row_values])
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    raise

    def _append_rows(self, sheet, rows: list[list[str]], max_retries: int, delay: int, chunk_size: int = 500) -> None:
        """Append rows in chunks to stay within the Sheets write-quota."""
        for chunk_start in range(0, len(rows), chunk_size):
            chunk = rows[chunk_start : chunk_start + chunk_size]
            for attempt in range(max_retries):
                try:
                    sheet.append_rows(chunk, value_input_option="USER_ENTERED")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            "Append rows retry %s/%s: %s", attempt + 1, max_retries, e
                        )
                        time.sleep(delay)
                    else:
                        raise
            # Brief pause between chunks.
            if chunk_start + chunk_size < len(rows):
                time.sleep(2)
    
    def _ensure_sheet_size(self, sheet, data_rows, data_cols, start_row, start_col, include_header):
        required_rows = start_row + data_rows - 1 + (1 if include_header else 0)
        required_cols = start_col + data_cols - 1

        target_rows = max(required_rows, 1)
        target_cols = max(required_cols, 1)

        if target_rows != sheet.row_count or target_cols != sheet.col_count:
            sheet.resize(rows=target_rows, cols=target_cols)

    def _import_sync(self, sheet, df, max_retries, delay, row, col, include_header, resize):
        for attempt in range(max_retries):
            try:
                set_with_dataframe(
                    sheet, df, row=row, col=col,
                    include_column_header=include_header,
                    resize=resize
                )
                logger.info("Imported %s rows successfully", len(df))
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning("Retry %s importing dataframe: %s", attempt + 1, e)
                    time.sleep(delay)
                else:
                    raise
        return False
    
    def _import_async(self, sheet, df, chunk_size, max_retries, delay, row, col, include_header, resize):
        total_rows = len(df)
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            fragment = df.iloc[start:end]
            start_row = row if start == 0 else row + start + (1 if include_header else 0)
            header_flag = include_header and start == 0
            try:
                self._import_fragment(
                    sheet, fragment, start_row, header_flag, max_retries, delay, False
                )
            except Exception as e:
                logger.exception("Chunk error while importing dataframe: %s", e)
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
            logger.exception("Worksheet not found: %s", e)
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
                        logger.exception("Failed to update %s: %s", col_name, e)
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
