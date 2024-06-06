WITH RankedSalaries AS (
    SELECT 
        emp_id, 
        start_date, 
        end_date, 
        salary,
        ROW_NUMBER() OVER (PARTITION BY emp_id, start_date, end_date ORDER BY salary DESC) AS rn
    FROM emp_timeframe_final_table
)
DELETE FROM emp_timeframe_final_table
WHERE (emp_id, start_date, end_date, salary) IN (
    SELECT emp_id, start_date, end_date, salary FROM RankedSalaries WHERE rn > 1
);
