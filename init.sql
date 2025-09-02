-- Database initialization script

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_employee_datetime ON employees(datetime);
CREATE INDEX IF NOT EXISTS idx_employee_department ON employees(department_id);
CREATE INDEX IF NOT EXISTS idx_employee_job ON employees(job_id);
CREATE INDEX IF NOT EXISTS idx_employee_dept_job ON employees(department_id, job_id);
