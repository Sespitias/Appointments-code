# Unmapped Reason Services Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `Service` its correct value for `Followup Televisit Rt` (`Follow up`) and every `First Office Visit <State>` reason (`First Office`) in new and historical rows of `appointment_prod`, so weekly reports and retention queries filtering by service stop dropping these appointments.

**Architecture:** `apply_column_service` resolves `Service` from `build_service_map()`: the local catalog `APPOINTMENT_REASON_SERVICE_MAP`, overridden by the Master sheet. Matching is exact on a trimmed, casefolded key. This plan adds the four reasons to the local catalog and a prefix fallback (`first office…` → `First Office`, `followup televisit…` → `Follow up`). The fallback applies only when the exact lookup misses, so the next new state (e.g. `First Office Visit Arizona`) does not break the report again. A one-off SQL backfill fixes rows outside the pipeline window.

**Tech Stack:** Python 3.11, pandas, `unittest` (`test_pipeline.py`), BigQuery SQL.

**Spec:** Request from 2026-10-20 (this conversation). Evidence checked in `weekly-revenue-integration.Telemind_BD.appointment_prod` on 2026-10-20:

| AppointmentReason1 | Service now | Rows | StartOnlyDate range | Correct Service |
|---|---|---|---|---|
| First Office Visit Nevada | NULL / 'nan' | 253 + 27 | 2026-09-16 → 2026-11-04 | First Office |
| First Office Visit Texas | NULL / 'nan' | 30 + 8 | 2026-09-16 → 2026-10-30 | First Office |
| First Office Visit New Mexico | NULL | 3 | 2026-09-29 → 2026-10-09 | First Office |
| First Office Texas TeleVisit | 'nan' | 1 | 2026-03-02 | First Office |
| Followup Televisit Rt | NULL | 12 | 2026-10-16 → 2026-10-29 | Follow up |
| Follow up + Therapy | 'nan' | 24 | 2026-02-25 → 2026-03-02 | Follow up |
| TMS Treatmeant / TMS Reconfiguration | 'nan' | 10 / 1 | 2026-02-25 | TMS |
| TMS Clearance | 'nan' | 2 | 2026-02-25 | Follow up (Master: `Tms Clearance → Follow up`) |
| ADD Determination Interview | 'nan' | 1 | 2026-02-25 | Follow up |
| Follow Up Pw | 'nan' | 2 | 2025-04/05 | Follow up |

The last five rows of the table are casing mismatches with Master. Commit `7c7bbaa` (case-insensitive matching) already fixes them for new data, but they are outside the pipeline window (`months_back: 1`), so they only need the backfill.

## Global Constraints

- Work on top of branch `fix/mindmetrix-service` (deployed to Cloud Run, not yet merged). Create branch `fix/unmapped-reason-services` from it.
- Service values must match the existing ones exactly: `First Office`, `Follow up`, `TMS`.
- Master sheet always wins over the local catalog and the prefix rules. Exact matches always win over prefix rules.
- Internal calendar blocks (`Meeting`, `Uds`, `Office Closed`, `Dinner`, `Ooto`) intentionally have no Service and must stay NULL.
- Tests: `venv/Scripts/python.exe -m unittest test_pipeline -v` from the repo root.
- Conventional commits, no `Co-Authored-By` or AI attribution. Code, comments and logs in English.
- Production actions (Cloud Run image update, BigQuery `UPDATE`) only with explicit human approval in chat.

## Review Focus

1. A state that does not exist yet (`First Office Visit Arizona`) must resolve to `First Office` without a code change. Test in Task 1.
2. Casing and whitespace (`" FIRST OFFICE VISIT NEVADA "`, `"followup televisit rt"`) must still match. Test in Task 1.
3. A Master row for a prefix-matching reason with a different Service (e.g. `First Office Visit Utah → Special`) must win over the prefix rule. Test in Task 1.
4. Internal blocks (`Meeting`) and unrelated reasons (`Therapy` with no Master row) must not be captured by a prefix rule and must stay unmapped. Test in Task 1.
5. Combined reasons such as `First Office Visit, Followup Televisit` must resolve to `First Office`, not `Follow up`. The prefix order is irrelevant here because the reason starts with "first office". Test in Task 1.

---

### Task 1: Catalog entries and prefix fallback for Service

**Files:**
- Modify: `column_registry.py` (`APPOINTMENT_REASON_SERVICE_MAP`; add prefix rules + resolver after `build_service_map`)
- Modify: `transformations.py` (`apply_column_service`, lines ~111-143)
- Modify: `Campos.json` (`Service.rules`: add "Reasons starting with 'First Office' map to First Office and 'Followup Televisit' to Follow up when no exact mapping exists.")
- Test: `test_pipeline.py` (class `TransformationTests`)

**Interfaces:**
- Consumes: `normalize_reason_key(value) -> str | None`, `build_service_map(master_df=None) -> dict[str, str]`, `is_missing_text(value) -> bool` (all existing in `column_registry.py`).
- Produces:
  - `APPOINTMENT_REASON_SERVICE_MAP` gains `"Followup Televisit Rt": "Follow up"`, `"First Office Visit Nevada": "First Office"`, `"First Office Visit Texas": "First Office"`, `"First Office Visit New Mexico": "First Office"`.
  - `SERVICE_PREFIX_RULES: tuple[tuple[str, str], ...] = (("first office", "First Office"), ("followup televisit", "Follow up"))`. Prefixes are already normalized keys.
  - `resolve_service(reason, service_map: dict[str, str]) -> str | None`. It normalizes `reason`, returns `service_map[key]` on an exact hit, otherwise the Service of the first rule whose prefix `key.startswith(...)`, otherwise `None`.
  - `apply_column_service(df, master_df) -> pd.DataFrame`, same signature. The `mapped_service` line uses `df["AppointmentReason1"].map(lambda r: resolve_service(r, service_map))`.

- [ ] **Step 1: Write the failing tests**

```python
def test_apply_column_service_maps_new_first_office_states(self):
    reasons = ["First Office Visit Nevada", "First Office Visit Texas",
               "First Office Visit New Mexico", "First Office Visit Arizona",
               " FIRST OFFICE VISIT NEVADA ", "First Office Visit, Followup Televisit"]
    df = pd.DataFrame({"AppointmentReason1": reasons})
    master_df = pd.DataFrame({"AppointmentReason1": ["Followup Televisit"], "Service": ["Follow up"]})
    result = apply_column_service(df, master_df)
    self.assertEqual(result["Service"].tolist(), ["First Office"] * 6)

def test_apply_column_service_maps_followup_televisit_rt(self):
    df = pd.DataFrame({"AppointmentReason1": ["Followup Televisit Rt", "followup televisit rt"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["Known"], "Service": ["Mapped"]})
    result = apply_column_service(df, master_df)
    self.assertEqual(result["Service"].tolist(), ["Follow up", "Follow up"])

def test_apply_column_service_master_beats_prefix_rule(self):
    df = pd.DataFrame({"AppointmentReason1": ["First Office Visit Utah"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["First Office Visit Utah"], "Service": ["Special"]})
    result = apply_column_service(df, master_df)
    self.assertEqual(result.loc[0, "Service"], "Special")

def test_apply_column_service_prefix_rules_skip_unrelated_reasons(self):
    df = pd.DataFrame({"AppointmentReason1": ["Meeting", "Therapy Session", "Office Closed"]})
    master_df = pd.DataFrame({"AppointmentReason1": ["Meeting"], "Service": [""]})
    result = apply_column_service(df, master_df)
    self.assertTrue(result["Service"].isna().all())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `venv/Scripts/python.exe -m unittest test_pipeline.TransformationTests -v`
Expected: the first two new tests FAIL (`None != 'First Office'` / `None != 'Follow up'`). The last two pass already and stay as regression guards. All pre-existing tests pass.

- [ ] **Step 3: Implement the catalog entries, `SERVICE_PREFIX_RULES` and `resolve_service` in `column_registry.py`, and switch `apply_column_service` to `resolve_service`**

- [ ] **Step 4: Run the full suite**

Run: `venv/Scripts/python.exe -m unittest test_pipeline -v`
Expected: all tests PASS (48 existing + 4 new = 52).

- [ ] **Step 5: Commit**

```bash
git add column_registry.py transformations.py Campos.json test_pipeline.py
git commit -m "fix: map First Office state visits and Followup Televisit Rt to services"
```

---

### Task 2: Backfill historical rows

**Files:**
- Create: `scripts/backfill_unmapped_reason_services.sql`

**Interfaces:**
- Consumes: the Service values from Task 1 and the casing-mismatch reasons in the Spec table.

- [ ] **Step 1: Write the SQL file**

Header comments: the purpose, "Run after the Task 1 image is deployed", "Run outside a pipeline execution (job runs every ~15 min; check `gcloud run jobs executions list`)", and "Run the preview first".

The preview `SELECT` and the `UPDATE` share one mapping expression over `k = LOWER(TRIM(AppointmentReason1))`:

```sql
CASE
  WHEN STARTS_WITH(k, 'first office') THEN 'First Office'
  WHEN STARTS_WITH(k, 'followup televisit') THEN 'Follow up'
  WHEN k IN ('follow up + therapy', 'follow up pw', 'add determination interview', 'tms clearance') THEN 'Follow up'
  WHEN k IN ('tms treatmeant', 'tms reconfiguration', 'tms mapping (initial)') THEN 'TMS'
END
```

The preview groups by `AppointmentReason1`, current `Service` and new Service. The `UPDATE` sets `Service` to that expression `WHERE (Service IS NULL OR LOWER(TRIM(Service)) IN ('', 'nan', 'none'))` and the expression `IS NOT NULL`, so internal blocks stay untouched.

- [ ] **Step 2: Dry-run the UPDATE**

Run it with BigQuery `dryRun: true`.
Expected: valid query, no rows changed.

- [ ] **Step 3: Commit**

```bash
git add scripts/backfill_unmapped_reason_services.sql
git commit -m "chore: add backfill for unmapped reason services in appointment_prod"
```

---

### Task 3: Deploy and backfill (human-approved)

**Files:** none.

- [ ] **Step 1: Build the image from the branch** — `gcloud builds submit --project weekly-revenue-integration --tag gcr.io/weekly-revenue-integration/appointment-pipeline:<short-sha> .`. Expected: `SUCCESS` and a digest.
- [ ] **Step 2: With approval, point the job to that digest** — `gcloud run jobs update appointment-pipeline --region us-central1 --project weekly-revenue-integration --image gcr.io/weekly-revenue-integration/appointment-pipeline@<digest>`. Rollback target: the current digest `sha256:2353596b…`.
- [ ] **Step 3: Run the preview `SELECT`.** Expected: about 374 rows to change (Spec table minus internal blocks and the `nan`/`Cambioname` rows). No preview row has a new Service other than `First Office`, `Follow up` or `TMS`.
- [ ] **Step 4: With approval and no execution running, run the `UPDATE`.** Expected: the affected row count equals the preview total.
- [ ] **Step 5: Verify.** Re-run the Spec query (`Service IS NULL OR LOWER(TRIM(Service)) IN ('', 'nan', 'none')`, grouped by reason). Expected: only `Meeting`, `Uds`, `Office Closed`, `Dinner`, `Ooto`, `nan` and `Cambioname` remain. After the next pipeline execution, new `First Office Visit *` and `Followup Televisit Rt` rows arrive with Service set.

## Out of scope

- Adding these reasons to the Master sheet. That remains the recommended business-side follow-up, and Master values win once they exist.
- Rows in the Google Sheet `Appointment` tab outside the pipeline window.
- Downstream retention/weekly report queries. They benefit automatically once `Service` is filled.
