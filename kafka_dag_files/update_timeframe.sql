--- update status
UPDATE employee_timeframe_final_table
SET status = 'INACTIVE',end_date = current_date
WHERE status = 'ACTIVE' AND
emp_id IN (SELECT emp_id FROM employee_strikes WHERE strike_count>=10 );

DELETE FROM employee_strikes
WHERE strike_count = 0 or strike_count >= 10;

-- Step 3: Delete employees from SCD_employee_strikes that do not exist in employee_strikes
DELETE FROM SCD_employee_strikes
WHERE employee_id NOT IN (SELECT employee_id FROM employee_strikes);

DELETE FROM employee_messages
WHERE time_stamp::date < CURRENT_DATE;
