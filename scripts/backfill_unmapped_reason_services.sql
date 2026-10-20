-- Backfill Service for appointments whose reason had no mapping when written
-- (First Office Visit <State>, Followup Televisit Rt, and casing mismatches
-- such as 'Follow up + Therapy', 'TMS Treatmeant', 'TMS Clearance').
-- Run after the image with the prefix-rule fix is deployed.
-- Run outside a pipeline execution (job runs every ~15 min; check
-- `gcloud run jobs executions list`) to avoid conflicting with the MERGE job.
-- Run the preview first; run the UPDATE only after reviewing its output.
-- Internal blocks (Meeting, Uds, Office Closed, ...) map to NULL and are left untouched.

-- Preview
SELECT
  AppointmentReason1,
  Service AS current_service,
  new_service,
  COUNT(*) AS n
FROM (
  SELECT
    AppointmentReason1,
    Service,
    CASE
      WHEN STARTS_WITH(LOWER(TRIM(AppointmentReason1)), 'first office') THEN 'First Office'
      WHEN STARTS_WITH(LOWER(TRIM(AppointmentReason1)), 'followup televisit') THEN 'Follow up'
      WHEN LOWER(TRIM(AppointmentReason1)) IN ('follow up + therapy', 'follow up pw', 'add determination interview', 'tms clearance') THEN 'Follow up'
      WHEN LOWER(TRIM(AppointmentReason1)) IN ('tms treatmeant', 'tms reconfiguration', 'tms mapping (initial)') THEN 'TMS'
    END AS new_service
  FROM `weekly-revenue-integration.Telemind_BD.appointment_prod`
  WHERE Service IS NULL OR LOWER(TRIM(Service)) IN ('', 'nan', 'none')
)
WHERE new_service IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 3, 1;

-- Fix
UPDATE `weekly-revenue-integration.Telemind_BD.appointment_prod`
SET Service = CASE
      WHEN STARTS_WITH(LOWER(TRIM(AppointmentReason1)), 'first office') THEN 'First Office'
      WHEN STARTS_WITH(LOWER(TRIM(AppointmentReason1)), 'followup televisit') THEN 'Follow up'
      WHEN LOWER(TRIM(AppointmentReason1)) IN ('follow up + therapy', 'follow up pw', 'add determination interview', 'tms clearance') THEN 'Follow up'
      WHEN LOWER(TRIM(AppointmentReason1)) IN ('tms treatmeant', 'tms reconfiguration', 'tms mapping (initial)') THEN 'TMS'
    END
WHERE (Service IS NULL OR LOWER(TRIM(Service)) IN ('', 'nan', 'none'))
  AND CASE
      WHEN STARTS_WITH(LOWER(TRIM(AppointmentReason1)), 'first office') THEN 'First Office'
      WHEN STARTS_WITH(LOWER(TRIM(AppointmentReason1)), 'followup televisit') THEN 'Follow up'
      WHEN LOWER(TRIM(AppointmentReason1)) IN ('follow up + therapy', 'follow up pw', 'add determination interview', 'tms clearance') THEN 'Follow up'
      WHEN LOWER(TRIM(AppointmentReason1)) IN ('tms treatmeant', 'tms reconfiguration', 'tms mapping (initial)') THEN 'TMS'
    END IS NOT NULL;
