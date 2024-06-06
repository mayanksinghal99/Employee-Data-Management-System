INSERT INTO emp_timeframe_final_table (emp_id, designation, start_date, end_date, salary, status, processed_date)
SELECT emp_id, designation, start_date, end_date, salary, status, processed_date
FROM emp_timeframe_staging_table;
