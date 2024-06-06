WITH new_end_dates AS (
    SELECT
        emp_id,
        MIN(start_date) AS min_start_date
    FROM emp_timeframe_staging_table
    GROUP BY emp_id
)
UPDATE emp_timeframe_final_table
SET end_date = new_end_dates.min_start_date,
status = 'INACTIVE'
FROM new_end_dates
WHERE emp_timeframe_final_table.emp_id = new_end_dates.emp_id
AND emp_timeframe_final_table.end_date IS NULL;
