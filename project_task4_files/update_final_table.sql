-- Insert unique records from staging to leave_applications table for the current year
INSERT INTO leave_applications (emp_id, date, status)
SELECT emp_id, date, status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY emp_id, date ORDER BY 
           CASE 
               WHEN status = 'ACTIVE' THEN 1 
               WHEN status = 'CANCELLED' THEN 2 
           END) as rnk
    FROM leave_applications_staging
    WHERE extract(year from date) >= extract(year from CURRENT_DATE)
) subquery
WHERE rnk = 1;


-- Archive records from staging to leave_applications_archive table for previous years
INSERT INTO leave_applications_archive (emp_id, date, status)
SELECT emp_id, date, status
FROM leave_applications_staging
WHERE extract(year from date) < extract(year from CURRENT_DATE);

-- Delete old records from leave_applications table
DELETE FROM leave_applications
WHERE extract(year from date) < extract(year from CURRENT_DATE);
