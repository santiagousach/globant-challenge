import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from app.models import Department, Job, Employee
from datetime import datetime

@pytest.fixture
def setup_test_data(test_db: Session):
    """Setup test data for metrics tests"""
    # Create departments
    dept1 = Department(id=1, department="Engineering")
    dept2 = Department(id=2, department="Sales")
    dept3 = Department(id=3, department="Marketing")
    test_db.add_all([dept1, dept2, dept3])

    # Create jobs
    job1 = Job(id=1, job="Software Engineer")
    job2 = Job(id=2, job="Sales Manager")
    job3 = Job(id=3, job="Marketing Specialist")
    test_db.add_all([job1, job2, job3])

    # Create employees with 2021 dates
    employees = [
        Employee(id=1, name="John Doe", datetime=datetime(2021, 1, 15), department_id=1, job_id=1),
        Employee(id=2, name="Jane Smith", datetime=datetime(2021, 2, 20), department_id=1, job_id=1),
        Employee(id=3, name="Bob Johnson", datetime=datetime(2021, 4, 10), department_id=2, job_id=2),
        Employee(id=4, name="Alice Brown", datetime=datetime(2021, 7, 5), department_id=2, job_id=2),
        Employee(id=5, name="Charlie Wilson", datetime=datetime(2021, 10, 12), department_id=3, job_id=3),
        Employee(id=6, name="Diana Davis", datetime=datetime(2021, 11, 8), department_id=3, job_id=3),
        # Add employees for other years to test filtering
        Employee(id=7, name="Eve Miller", datetime=datetime(2022, 1, 15), department_id=1, job_id=1),
    ]
    test_db.add_all(employees)
    test_db.commit()

def test_hiring_by_quarter(client: TestClient, setup_test_data):
    """Test hiring by quarter endpoint"""
    response = client.get("/api/v1/metrics/hiring-by-quarter")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    # Check that we get expected structure
    if data:  # Only if there are results
        first_result = data[0]
        assert "department" in first_result
        assert "job" in first_result
        assert "Q1" in first_result
        assert "Q2" in first_result
        assert "Q3" in first_result
        assert "Q4" in first_result

        # Check specific expected values
        engineering_software = next(
            (item for item in data if item["department"] == "Engineering" and item["job"] == "Software Engineer"),
            None
        )
        if engineering_software:
            assert engineering_software["Q1"] == 2  # Jan and Feb hires
            assert engineering_software["Q2"] == 0
            assert engineering_software["Q3"] == 0
            assert engineering_software["Q4"] == 0

def test_departments_above_average(client: TestClient, setup_test_data):
    """Test departments above average hiring"""
    response = client.get("/api/v1/metrics/departments-above-average")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    # Check structure
    if data:
        first_result = data[0]
        assert "id" in first_result
        assert "department" in first_result
        assert "hired" in first_result

        # Verify ordering (descending by hired count)
        for i in range(len(data) - 1):
            assert data[i]["hired"] >= data[i + 1]["hired"]

def test_empty_database_metrics(client: TestClient):
    """Test metrics endpoints with empty database"""
    response = client.get("/api/v1/metrics/hiring-by-quarter")
    assert response.status_code == 200
    assert response.json() == []

    response = client.get("/api/v1/metrics/departments-above-average")
    assert response.status_code == 200
    assert response.json() == []

def test_health_check(client: TestClient):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_root_endpoint(client: TestClient):
    """Test root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "Globant Data Engineering Challenge API" in data["message"]
    assert "docs" in data
