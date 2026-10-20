# Mindmetrix Service Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cognitive appointments that Tebra now reports as `Mindmetrix` / `Mindmetrix Retake` must land in BigQuery with `Service = 'Cognitive'` instead of the string `'nan'`, and no unmapped string column may ever be written as `'nan'` again.

**Architecture:** `apply_column_service` maps `AppointmentReason1 → Service` only from the Google Sheet `Master` tab, with an exact, case-sensitive match. Mindmetrix reasons are not in Master, so the map returns `NaN`, and `bigquery_ops.CONVERSION_MAPPING` casts it with `str`, producing the literal `'nan'`. The fix adds a local fallback reason→service catalog in `column_registry.py` (same pattern as `CONFIRMATION_STATUS_DONE_MAP`: Master overrides local), matches reasons case-insensitively, makes the BQ string cast null-safe, and backfills rows already written.

**Tech Stack:** Python 3, pandas, pandas-gbq, google-cloud-bigquery, `unittest` (`test_pipeline.py`).

**Spec:** No spec doc. Evidence gathered on 2026-10-20:
- `appointment_prod` (project `weekly-revenue-integration`, dataset `Telemind_BD`): since 2026-09, `Mindmetrix` (147 rows) and `Mindmetrix Retake` (609 rows) have `Service = 'nan'`; `Cognitive Test` / `Cognitive Test Retake` → `Cognitive` until then.
- Master sheet (`SHEET_KEY`, tab `Master`) has `Cognitive Test → Cognitive` and `Cognitive Test Retake → Cognitive`, and no Mindmetrix row.
- Other reasons also land as `'nan'` for the same reason (`First Office Visit Nevada`, `First Office Visit Texas`, `Followup Televisit Rt`, `TMS Clearance` vs Master `Tms Clearance`, …). Their mapping is out of scope; Task 2 makes them `NULL` instead of `'nan'` and logs them.

## Global Constraints

- Mindmetrix reasons map to the existing service value `Cognitive` (exact string), so dashboards filtering `Service = 'Cognitive'` keep continuity.
- Master sheet values always override the local fallback catalog.
- Reason matching ignores case and leading/trailing whitespace.
- Test command: `python -m unittest test_pipeline -v` (run from repo root, inside `venv`).
- Commits: conventional commits, no AI attribution / `Co-Authored-By` lines.
- Code, comments, and log messages in English.

## Review Focus

1. Casing/whitespace variants from Tebra or the sheet merge path (`MindMetrix`, `MINDMETRIX RETAKE`, `" Mindmetrix "`) must still resolve to `Cognitive` — test in Task 1.
2. Someone later adds Mindmetrix to Master with a different Service — Master must win — test in Task 1.
3. A pre-existing `Service` of `'nan'`, `'None'`, or `''` (rows coming back from the Appointment sheet) must be treated as missing, not preserved — test in Task 1.
4. Reasons still absent from both catalogs must become `NULL` in BQ (not `'nan'`) and appear in a warning log — tests in Task 1 and Task 2.
5. Rows older than the pipeline window (`months_back: 1`) are never reprocessed, so September rows stay `'nan'` unless backfilled — Task 3.

---

### Task 1: Reason→Service fallback catalog with case-insensitive matching

**Files:**
- Modify: `column_registry.py` (add catalog + builder after `build_done_map`)
- Modify: `transformations.py:110-121` (`apply_column_service`)
- Modify: `Campos.json` (`AppointmentReason1.example_values` add `"Mindmetrix"`; `Service.rules` add "Mindmetrix reasons map to Cognitive via local fallback catalog")
- Test: `test_pipeline.py` (class `TransformationTests`)

**Interfaces:**
- Produces:
  - `column_registry.APPOINTMENT_REASON_SERVICE_MAP: dict[str, str] = {"Mindmetrix": "Cognitive", "Mindmetrix Retake": "Cognitive"}`
  - `column_registry.normalize_reason_key(value) -> str | None` — `str(value).strip().casefold()`; returns `None` for `None`/NaN/blank.
  - `column_registry.build_service_map(master_df=None) -> dict[str, str]` — keys are normalized reasons; local catalog first, then Master rows (columns `AppointmentReason1`, `Service`) override. Missing columns → warning, local only.
  - `apply_column_service(df, master_df) -> pd.DataFrame` — same signature as today.

- [ ] **Step 1: Write the failing tests**

```python
def test_apply_column_service_maps_mindmetrix_to_cognitive(self):
    df = pd.DataFrame({"AppointmentReason1": ["Mindmetrix", "Mindmetrix Retake"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["Cognitive Test"], "Service": ["Cognitive"]})
    result = apply_column_service(df, master_df)
    self.assertEqual(result["Service"].tolist(), ["Cognitive", "Cognitive"])

def test_apply_column_service_matches_reason_case_insensitively(self):
    df = pd.DataFrame({"AppointmentReason1": ["MindMetrix", " MINDMETRIX RETAKE ", "tms clearance"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["Tms Clearance"], "Service": ["Follow up"]})
    result = apply_column_service(df, master_df)
    self.assertEqual(result["Service"].tolist(), ["Cognitive", "Cognitive", "Follow up"])

def test_apply_column_service_master_overrides_local_catalog(self):
    df = pd.DataFrame({"AppointmentReason1": ["Mindmetrix"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["Mindmetrix"], "Service": ["Neuro"]})
    result = apply_column_service(df, master_df)
    self.assertEqual(result.loc[0, "Service"], "Neuro")

def test_apply_column_service_treats_nan_string_service_as_missing(self):
    df = pd.DataFrame(
        {"AppointmentReason1": ["Unknown", "Unknown", "Unknown"], "Service": ["nan", "None", ""]}
    )
    master_df = pd.DataFrame({"AppointmentReason1": ["Known"], "Service": ["Mapped"]})
    result = apply_column_service(df, master_df)
    self.assertTrue(result["Service"].isna().all())

def test_apply_column_service_logs_unmapped_reasons(self):
    df = pd.DataFrame({"AppointmentReason1": ["First Office Visit Nevada"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["Known"], "Service": ["Mapped"]})
    with self.assertLogs("transformations", level="WARNING") as logs:
        apply_column_service(df, master_df)
    self.assertIn("First Office Visit Nevada", "".join(logs.output))
```

The existing `test_apply_column_service_preserves_existing_service_when_mapping_missing` must keep passing (a real existing value like `"Keep Me"` is still preserved).

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest test_pipeline.TransformationTests -v`
Expected: the 5 new tests FAIL (Service is NaN / no warning); existing tests PASS.

- [ ] **Step 3: Implement catalog + builder in `column_registry.py`, then rewrite `apply_column_service` in `transformations.py`**

`apply_column_service`: build the map with `build_service_map(master_df)`, look up `df["AppointmentReason1"].map(normalize_reason_key).map(service_map)`. Clean the existing `Service` by turning `"nan"`, `"None"`, `""` (after strip) into `None` before `fillna`. After filling, log one warning listing the unique `AppointmentReason1` values whose `Service` is still null (message format mirrors `apply_column_done`: `"AppointmentReason1 values not in Service catalog: %s"`). Import the new names alongside `build_done_map`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest test_pipeline -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add column_registry.py transformations.py Campos.json test_pipeline.py
git commit -m "fix: map Mindmetrix reasons to Cognitive service"
```

---

### Task 2: Null-safe string conversion before BigQuery upload

**Files:**
- Modify: `bigquery_ops.py:38-63` (`CONVERSION_MAPPING`) and add helper above it
- Test: `test_pipeline.py` (class `BigQueryOpsTests`)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `bigquery_ops.to_nullable_str(series: pd.Series) -> pd.Series` — values that are `None`, NaN, or whose stripped text is `"nan"`, `"None"`, `"NaT"`, or `""` become `None`; everything else becomes `str(value)`. Every `str` entry in `CONVERSION_MAPPING` (ID, ConfirmationStatus, ServiceLocationName, PatientID, PatientFullName, PatientCaseID, PatientCaseName, PatientCasePayerScenario, AppointmentReason1, Provider, Service, WeekDate, MonthDate, CreationWeekDate, CaseNameID) is replaced by `to_nullable_str`.

- [ ] **Step 1: Write the failing tests**

```python
def test_convert_columns_writes_null_instead_of_nan_string(self):
    df = pd.DataFrame({"Service": [float("nan"), None, "nan", "Cognitive"]})
    result = convert_columns(df)
    self.assertEqual(result["Service"].tolist(), [None, None, None, "Cognitive"])

def test_convert_columns_keeps_ids_as_strings(self):
    df = pd.DataFrame({"ID": [123, "456"]})
    result = convert_columns(df)
    self.assertEqual(result["ID"].tolist(), ["123", "456"])
```

Add `convert_columns` to the `from bigquery_ops import (...)` block.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest test_pipeline.BigQueryOpsTests -v`
Expected: `test_convert_columns_writes_null_instead_of_nan_string` FAILS with `'nan' != None`.

- [ ] **Step 3: Implement `to_nullable_str` and swap it into `CONVERSION_MAPPING`**

Return a Series with `dtype=object` so pandas-gbq uploads `None` as BigQuery `NULL`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest test_pipeline -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bigquery_ops.py test_pipeline.py
git commit -m "fix: upload missing string values as NULL instead of 'nan'"
```

Note for reviewer: the `MERGE` change detection in `merge_appointments` uses `COALESCE(CAST(... AS STRING), '')`, so rows going from `'nan'` to `NULL` will register as changed and be updated once. That is expected.

---

### Task 3: Backfill historical rows in BigQuery

**Files:**
- Create: `scripts/backfill_mindmetrix_service.sql`

**Interfaces:**
- Consumes: the value `Cognitive` and the reason list from Task 1's `APPOINTMENT_REASON_SERVICE_MAP`.

- [ ] **Step 1: Write the preview query and the update in the SQL file**

```sql
-- Preview
SELECT AppointmentReason1, Service, COUNT(*) AS n
FROM `weekly-revenue-integration.Telemind_BD.appointment_prod`
WHERE LOWER(TRIM(AppointmentReason1)) IN ('mindmetrix', 'mindmetrix retake')
GROUP BY 1, 2;

-- Fix
UPDATE `weekly-revenue-integration.Telemind_BD.appointment_prod`
SET Service = 'Cognitive'
WHERE LOWER(TRIM(AppointmentReason1)) IN ('mindmetrix', 'mindmetrix retake')
  AND (Service IS NULL OR TRIM(Service) IN ('', 'nan', 'None'));
```

- [ ] **Step 2: Run the preview query**

Expected: rows with `Service = 'nan'` (≈756 as of 2026-10-20), none with another non-null Service.

- [ ] **Step 3: Run the UPDATE — only after the human partner explicitly approves (it mutates production data)**

Expected: affected row count equals the preview's `'nan'` total; re-running the preview shows only `Service = 'Cognitive'`.

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_mindmetrix_service.sql
git commit -m "chore: add backfill for Mindmetrix service in appointment_prod"
```

---

## Out of scope (follow-ups)

- Add `Mindmetrix` / `Mindmetrix Retake` rows to the Master sheet so the business catalog is the single source of truth (the code fallback covers it meanwhile).
- Map the other reasons currently unmapped (`First Office Visit Nevada/Texas/New Mexico`, `Followup Televisit Rt`, `ADD Determination Interview`, `TMS *` uppercase variants). Task 1's case-insensitive match fixes the casing-only ones; the rest need Master rows. Task 1's warning log will list them on each run.
- Older rows in the `Appointment` Google Sheet tab that still carry an empty Service.
