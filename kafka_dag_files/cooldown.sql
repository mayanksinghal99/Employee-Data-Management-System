
UPDATE SCD_employee_strikes
SET
    sc_1 = CASE WHEN strike_count = 1 THEN NULL ELSE sc_1 END,
    sc_2 = CASE WHEN strike_count = 2 THEN NULL ELSE sc_2 END,
    sc_3 = CASE WHEN strike_count = 3 THEN NULL ELSE sc_3 END,
    sc_4 = CASE WHEN strike_count = 4 THEN NULL ELSE sc_4 END,
    sc_5 = CASE WHEN strike_count = 5 THEN NULL ELSE sc_5 END,
    sc_6 = CASE WHEN strike_count = 6 THEN NULL ELSE sc_6 END,
    sc_7 = CASE WHEN strike_count = 7 THEN NULL ELSE sc_7 END,
    sc_8 = CASE WHEN strike_count = 8 THEN NULL ELSE sc_8 END,
    sc_9 = CASE WHEN strike_count = 9 THEN NULL ELSE sc_9 END
WHERE
    CURRENT_DATE - last_strike_date = 30;

--- update strikes
UPDATE SCD_employee_strikes
SET last_strike_date = CURRENT_DATE,
    strike_count = strike_count - 1
WHERE CURRENT_DATE - last_strike_date = 30;

--- update salary and date
UPDATE employee_strikes
SET last_strike_date = CURRENT_DATE,
    new_salary = curr_salary * power(0.9,strike_count)
WHERE CURRENT_DATE - last_strike_date = 30;
