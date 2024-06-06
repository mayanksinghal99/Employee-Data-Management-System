INSERT INTO emp_final_table (name, age, emp_id, processed_date)
SELECT name, age, emp_id, processed_date
FROM emp_staging_table;

