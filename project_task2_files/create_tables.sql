CREATE TABLE IF NOT EXISTS emp_timeframe_staging_table (
        emp_id BIGINT,
        designation VARCHAR(255),
        start_date DATE,
        end_date DATE,
        salary NUMERIC,
        status VARCHAR(50),
        processed_date DATE
    );

    CREATE TABLE IF NOT EXISTS emp_timeframe_final_table (
        emp_id BIGINT,
        designation VARCHAR(255),
        start_date DATE,
        end_date DATE,
        salary NUMERIC,
        status VARCHAR(50),
        processed_date DATE
    );

    CREATE TABLE IF NOT EXISTS daily_active_employees (
        designation VARCHAR(255) PRIMARY KEY,
        active_employee_count BIGINT
    );
