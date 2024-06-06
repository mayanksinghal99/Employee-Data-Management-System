INSERT INTO daily_active_employees (designation, active_employee_count)
SELECT designation, COUNT(*) AS active_employee_count
FROM emp_timeframe_final_table
WHERE status = 'ACTIVE'
GROUP BY designation
ON CONFLICT (designation)
DO UPDATE SET active_employee_count = EXCLUDED.active_employee_count;
