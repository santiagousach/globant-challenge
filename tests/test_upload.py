import pytest
from io import BytesIO
from fastapi.testclient import TestClient

def test_upload_departments_csv(client: TestClient):
    """Test departments CSV upload"""
    csv_content = """1,Engineering
2,Sales
3,Marketing"""

    files = {"file": ("departments.csv", BytesIO(csv_content.encode()), "text/csv")}

    response = client.post("/api/v1/upload/departments", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["processed_rows"] == 3
    assert "Departments uploaded successfully" in data["message"]

def test_upload_jobs_csv(client: TestClient):
    """Test jobs CSV upload"""
    csv_content = """1,Software Engineer
2,Data Analyst
3,Manager"""

    files = {"file": ("jobs.csv", BytesIO(csv_content.encode()), "text/csv")}

    response = client.post("/api/v1/upload/jobs", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["processed_rows"] == 3
    assert "Jobs uploaded successfully" in data["message"]

def test_upload_employees_csv(client: TestClient):
    """Test employees CSV upload"""
    csv_content = """1,John Doe,2021-01-15T10:00:00Z,1,1
2,Jane Smith,2021-02-20T11:00:00Z,2,2
3,Bob Johnson,2021-03-10T12:00:00Z,1,3"""

    files = {"file": ("employees.csv", BytesIO(csv_content.encode()), "text/csv")}

    response = client.post("/api/v1/upload/employees", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["processed_rows"] == 3
    assert "Employees uploaded successfully" in data["message"]

def test_upload_invalid_csv_format(client: TestClient):
    """Test upload with invalid file format"""
    files = {"file": ("test.txt", BytesIO(b"test content"), "text/plain")}

    response = client.post("/api/v1/upload/departments", files=files)
    assert response.status_code == 400
    assert "File must be CSV format" in response.json()["detail"]

def test_upload_malformed_csv(client: TestClient):
    """Test upload with malformed CSV"""
    csv_content = "invalid,csv,content,without,proper,structure"

    files = {"file": ("departments.csv", BytesIO(csv_content.encode()), "text/csv")}

    response = client.post("/api/v1/upload/departments", files=files)
    assert response.status_code == 500  # Should handle gracefully

def test_upload_duplicate_departments(client: TestClient):
    """Test uploading departments with duplicates"""
    # First upload
    csv_content = "1,Engineering"
    files = {"file": ("departments.csv", BytesIO(csv_content.encode()), "text/csv")}
    client.post("/api/v1/upload/departments", files=files)

    # Second upload with same data
    response = client.post("/api/v1/upload/departments", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["processed_rows"] == 0  # No new records processed
