#!/usr/bin/env python3
"""
Demo script to showcase the Globant Data Engineering Challenge API
This script demonstrates how to upload the provided CSV files and query the analytics
"""

import requests
import time
from pathlib import Path

# API Configuration
BASE_URL = "http://localhost:8000"

def wait_for_service(max_attempts=30):
    """Wait for the API service to be ready"""
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{BASE_URL}/health")
            if response.status_code == 200:
                print("✅ API service is ready!")
                return True
        except requests.exceptions.ConnectionError:
            pass

        print(f"⏳ Waiting for API service... (attempt {attempt + 1}/{max_attempts})")
        time.sleep(2)

    print("❌ API service failed to start")
    return False

def upload_csv(endpoint: str, file_path: str):
    """Upload a CSV file to the specified endpoint"""
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return None

    print(f"📤 Uploading {Path(file_path).name} to {endpoint}...")

    with open(file_path, 'rb') as f:
        files = {'file': (Path(file_path).name, f, 'text/csv')}
        response = requests.post(f"{BASE_URL}/api/v1/upload/{endpoint}", files=files)

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Successfully uploaded {data['processed_rows']} rows")
        if data['errors']:
            print(f"⚠️  Errors: {data['errors']}")
        return data
    else:
        print(f"❌ Upload failed: {response.status_code} - {response.text}")
        return None

def query_metrics(endpoint: str):
    """Query metrics from the API"""
    print(f"📊 Fetching {endpoint}...")

    response = requests.get(f"{BASE_URL}/api/v1/metrics/{endpoint}")

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Retrieved {len(data)} records")
        return data
    else:
        print(f"❌ Query failed: {response.status_code} - {response.text}")
        return None

def display_hiring_metrics(data):
    """Display hiring by quarter metrics in a formatted way"""
    if not data:
        return

    print("\n📈 Hiring by Quarter (2021):")
    print("-" * 80)
    print("50")
    print("-" * 80)

    for item in data[:10]:  # Show first 10 results
        print("20")

    if len(data) > 10:
        print(f"... and {len(data) - 10} more records")

def display_departments_above_avg(data):
    """Display departments above average metrics"""
    if not data:
        return

    print("\n🏆 Departments Above Average Hiring (2021):")
    print("-" * 50)
    print("30")
    print("-" * 50)

    for item in data:
        print("20")

def main():
    """Main demo function"""
    print("🚀 Globant Data Engineering Challenge - API Demo")
    print("=" * 60)

    # Check if API is ready
    if not wait_for_service():
        return

    # Define CSV file paths (adjust these paths based on your setup)
    csv_files = {
        "departments": "/Users/santiago/Downloads/Globant/departments__1___1_.csv",
        "jobs": "/Users/santiago/Downloads/Globant/jobs.csv",
        "employees": "/Users/santiago/Downloads/Globant/hired_employees__1___1_.csv"
    }

    # Upload CSV files
    print("\n📁 Phase 1: Uploading CSV Files")
    print("-" * 40)

    upload_results = {}
    for endpoint, file_path in csv_files.items():
        result = upload_csv(endpoint, file_path)
        upload_results[endpoint] = result
        time.sleep(1)  # Small delay between uploads

    # Wait a moment for data processing
    print("\n⏳ Processing uploaded data...")
    time.sleep(2)

    # Query analytics
    print("\n📊 Phase 2: Querying Analytics")
    print("-" * 40)

    # Get hiring by quarter metrics
    hiring_data = query_metrics("hiring-by-quarter")
    if hiring_data:
        display_hiring_metrics(hiring_data)

    # Get departments above average
    dept_data = query_metrics("departments-above-average")
    if dept_data:
        display_departments_above_avg(dept_data)

    print("\n🎉 Demo completed!")
    print(f"🌐 API Documentation: {BASE_URL}/docs")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
