-- Backfill Service = 'Cognitive' for Mindmetrix appointments written as 'nan'.
-- Run outside the pipeline schedule (every 6h) to avoid conflicting with the MERGE job.
-- Run the preview first; run the UPDATE only after reviewing its output.

-- Preview
SELECT AppointmentReason1, Service, COUNT(*) AS n
FROM `weekly-revenue-integration.Telemind_BD.appointment_prod`
WHERE LOWER(TRIM(AppointmentReason1)) IN ('mindmetrix', 'mindmetrix retake')
GROUP BY 1, 2;

-- Fix
UPDATE `weekly-revenue-integration.Telemind_BD.appointment_prod`
SET Service = 'Cognitive'
WHERE LOWER(TRIM(AppointmentReason1)) IN ('mindmetrix', 'mindmetrix retake')
  AND (Service IS NULL OR LOWER(TRIM(Service)) IN ('', 'nan', 'none'));
