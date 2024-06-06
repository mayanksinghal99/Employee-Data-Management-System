CREATE TABLE IF NOT EXISTS emp_leave_quota_table (
        emp_id BIGINT NOT NULL,
        leave_quota INT NOT NULL,
        year INT NOT NULL
    );

CREATE TABLE IF NOT EXISTS leave_calendar_table (
        reason VARCHAR(50) NOT NULL,
        date DATE NOT NULL
    );
