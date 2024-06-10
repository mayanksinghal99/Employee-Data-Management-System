-- Update existing records in employee_strikes
UPDATE employee_strikes AS target
SET
    strike_count = target.strike_count + staging.strike_count,
    last_strike_date = staging.last_strike_date,
    salary = staging.salary,
    new_salary = ROUND(staging.salary * POWER(0.9,
        (staging.strike_count + target.strike_count)), 2
    )
FROM
    employee_messages_staging AS staging
WHERE
    target.employee_id = staging.employee_id;


-- Insert new records into employee_strikes
INSERT INTO employee_strikes (employee_id, strike_count, last_strike_date, salary, new_salary)
SELECT
    staging.employee_id,
    staging.strike_count,
    staging.last_strike_date,
    staging.salary,
    ROUND(staging.salary * POWER(0.9, staging.strike_count), 2) AS new_salary
FROM
    employee_messages_staging AS staging
WHERE
    staging.employee_id NOT IN (SELECT employee_id FROM employee_strikes);

