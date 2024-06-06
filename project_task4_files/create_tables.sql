CREATE TABLE IF NOT EXISTS leave_applications_staging (
            emp_id BIGINT,
            date DATE,
            status VARCHAR(20)
        );

CREATE TABLE IF NOT EXISTS leave_applications (
            emp_id BIGINT,
            date DATE,
            status VARCHAR(20)
        );

CREATE TABLE IF NOT EXISTS leave_applications_archive (
            emp_id BIGINT,
            date DATE,
            status VARCHAR(20)
        );
   
CREATE TABLE IF NOT EXISTS potential_leave_count (
            emp_id BIGINT,
            upcoming_leaves INT
        );
