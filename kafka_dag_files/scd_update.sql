
UPDATE SCD_employee_strikes
SET
    SC_1 = CASE WHEN strike_count >= 1 THEN ROUND(salary * POWER(0.9, 1), 2) ELSE 0 END,
    SC_2 = CASE WHEN strike_count >= 2 THEN ROUND(salary * POWER(0.9, 2), 2) ELSE 0 END,
    SC_3 = CASE WHEN strike_count >= 3 THEN ROUND(salary * POWER(0.9, 3), 2) ELSE 0 END,
    SC_4 = CASE WHEN strike_count >= 4 THEN ROUND(salary * POWER(0.9, 4), 2) ELSE 0 END,
    SC_5 = CASE WHEN strike_count >= 5 THEN ROUND(salary * POWER(0.9, 5), 2) ELSE 0 END,
    SC_6 = CASE WHEN strike_count >= 6 THEN ROUND(salary * POWER(0.9, 6), 2) ELSE 0 END,
    SC_7 = CASE WHEN strike_count >= 7 THEN ROUND(salary * POWER(0.9, 7), 2) ELSE 0 END,
    SC_8 = CASE WHEN strike_count >= 8 THEN ROUND(salary * POWER(0.9, 8), 2) ELSE 0 END,
    SC_9 = CASE WHEN strike_count >= 9 THEN ROUND(salary * POWER(0.9, 9), 2) ELSE 0 END;

