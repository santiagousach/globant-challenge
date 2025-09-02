# Globant Data Engineering Challenge

REST API for processing employee data and generating hiring analytics using FastAPI, PostgreSQL, and pandas for CSV processing.

- **CSV Upload**: Batch processing of employee, department, and job data
- **Analytics**: SQL-based metrics for hiring patterns
- **Docker**: Containerized deployment ready
- **Testing**: Comprehensive test suite included

### Prerequisites
- Docker and Docker Compose

### Run the Application
```bash
cd globant-challenge
docker-compose up --build
```

### Access Points
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## 📊 API Endpoints

### Upload Data
```bash
# Upload departments
curl -X POST "http://localhost:8000/api/v1/upload/departments" \
     -F "file=@departments.csv"

# Upload jobs
curl -X POST "http://localhost:8000/api/v1/upload/jobs" \
     -F "file=@jobs.csv"

# Upload employees (batch processing)
curl -X POST "http://localhost:8000/api/v1/upload/employees" \
     -F "file=@employees.csv"
```

### Get Analytics
```bash
# Hiring metrics by quarter (2021)
curl "http://localhost:8000/api/v1/metrics/hiring-by-quarter"

# Departments above average hiring
curl "http://localhost:8000/api/v1/metrics/departments-above-average"
```

## 🗃️ Database Schema

- **departments**: `id` (PK), `department` (unique)
- **jobs**: `id` (PK), `job` (unique)
- **employees**: `id` (PK), `name`, `datetime`, `department_id` (FK), `job_id` (FK)

## 🧪 Testing

```bash
# Run all tests
docker-compose exec api pytest

# Run specific tests
docker-compose exec api pytest tests/test_upload.py -v
```

