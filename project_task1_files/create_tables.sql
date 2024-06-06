CREATE TABLE IF NOT EXISTS emp_staging_table (
        name VARCHAR(100) NOT NULL,
        age INT NOT NULL,
        emp_id BIGINT NOT NULL,
        processed_date DATE
    );

CREATE TABLE IF NOT EXISTS emp_final_table (
        name VARCHAR(100) NOT NULL,
        age INT NOT NULL,
        emp_id BIGINT NOT NULL,
        processed_date DATE
    );
