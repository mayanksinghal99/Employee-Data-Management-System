
-- Update existing records in SCD_employee_strikes_table
UPDATE SCD_employee_strikes AS scd
SET
    strike_count = source.strike_count,
    last_strike_date = source.last_strike_date,
    salary = ROUND(source.salary / POWER(0.9, scd.strike_count), 2)
FROM employee_strikes AS source
WHERE scd.employee_id = source.employee_id;


INSERT INTO SCD_employee_strikes (
    employee_id, strike_count, last_strike_date, salary
)
SELECT
    employee_id,
    strike_count,
    last_strike_date,
    salary
FROM employee_strikes AS source
WHERE NOT EXISTS (
    SELECT 1 FROM SCD_employee_strikes AS scd
    WHERE scd.employee_id = source.employee_id
);