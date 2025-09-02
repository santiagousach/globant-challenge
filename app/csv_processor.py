import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from .models import Employee
import logging

logger = logging.getLogger(__name__)

class CSVProcessor:
    @staticmethod
    def process_departments_csv(file_content: bytes) -> List[Dict[str, Any]]:
        """Process departments CSV and return list of department dicts"""
        try:
            df = pd.read_csv(pd.io.common.BytesIO(file_content), header=None, names=['id', 'department'])

            # Clean and validate data
            df = df.dropna()
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            df['department'] = df['department'].astype(str).str.strip()
            df = df.dropna()

            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error processing departments CSV: {e}")
            raise ValueError("Invalid departments CSV format")

    @staticmethod
    def process_jobs_csv(file_content: bytes) -> List[Dict[str, Any]]:
        """Process jobs CSV and return list of job dicts"""
        try:
            df = pd.read_csv(pd.io.common.BytesIO(file_content), header=None, names=['id', 'job'])

            # Clean and validate data
            df = df.dropna()
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            df['job'] = df['job'].astype(str).str.strip()
            df = df.dropna()

            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error processing jobs CSV: {e}")
            raise ValueError("Invalid jobs CSV format")

    @staticmethod
    def process_employees_csv(file_content: bytes, batch_size: int = 1000) -> List[List[Dict[str, Any]]]:
        """Process employees CSV and return list of batches of employee dicts"""
        try:
            df = pd.read_csv(
                pd.io.common.BytesIO(file_content),
                header=None,
                names=['id', 'name', 'datetime', 'department_id', 'job_id']
            )

            # Clean and validate data
            df = df.dropna(subset=['name', 'datetime'])  # Required fields

            # Convert columns to proper types
            df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
            df['department_id'] = pd.to_numeric(df['department_id'], errors='coerce').astype('Int64')
            df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce').astype('Int64')

            # Validate ranges
            df = df[
                (df['department_id'].isna() | ((df['department_id'] >= 1) & (df['department_id'] <= 12))) &
                (df['job_id'].isna() | ((df['job_id'] >= 1) & (df['job_id'] <= 183)))
            ]

            # Convert datetime strings to datetime objects
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

            # Remove rows with invalid datetime or ID
            df = df.dropna(subset=['datetime', 'id'])

            # Convert nullable integers to regular integers for SQLAlchemy compatibility
            df['id'] = df['id'].astype(int)

            # Handle nullable columns properly
            def safe_nullable_int(value):
                return None if pd.isna(value) else int(value)

            df['department_id'] = df['department_id'].apply(safe_nullable_int)
            df['job_id'] = df['job_id'].apply(safe_nullable_int)

            # Split into batches
            batches = []
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]

                # Convert to dict and ensure proper Python types
                batch_records = []
                for _, row in batch_df.iterrows():
                    record = {
                        'id': int(row['id']),
                        'name': str(row['name']),
                        'datetime': row['datetime'].to_pydatetime() if pd.notna(row['datetime']) else None,
                        'department_id': int(row['department_id']) if pd.notna(row['department_id']) else None,
                        'job_id': int(row['job_id']) if pd.notna(row['job_id']) else None
                    }
                    batch_records.append(record)

                batches.append(batch_records)

            return batches
        except Exception as e:
            logger.error(f"Error processing employees CSV: {e}")
            raise ValueError("Invalid employees CSV format")

    @staticmethod
    def validate_employee_data(employee_data: Dict[str, Any]) -> bool:
        """Validate individual employee data"""
        required_fields = ['name', 'datetime']
        for field in required_fields:
            if not employee_data.get(field):
                return False

        # Validate datetime format
        if isinstance(employee_data['datetime'], str):
            try:
                datetime.fromisoformat(employee_data['datetime'].replace('Z', '+00:00'))
            except ValueError:
                return False

        return True

    @staticmethod
    def save_batch_to_db(db: Session, employees_data: List[Dict[str, Any]]) -> tuple[int, List[str]]:
        """Save batch of employees to database"""
        saved_count = 0
        errors = []

        for emp_data in employees_data:
            try:
                # Validate data
                if not CSVProcessor.validate_employee_data(emp_data):
                    errors.append(f"Invalid data for employee ID {emp_data.get('id', 'unknown')}")
                    continue

                # Check if employee already exists
                existing = db.query(Employee).filter(Employee.id == emp_data['id']).first()
                if existing:
                    continue  # Skip duplicates

                # Create employee record
                employee = Employee(
                    id=emp_data['id'],
                    name=emp_data['name'],
                    datetime=emp_data['datetime'] if isinstance(emp_data['datetime'], datetime)
                           else datetime.fromisoformat(emp_data['datetime'].replace('Z', '+00:00')),
                    department_id=emp_data.get('department_id'),
                    job_id=emp_data.get('job_id')
                )

                db.add(employee)
                saved_count += 1

            except Exception as e:
                errors.append(f"Error saving employee ID {emp_data.get('id', 'unknown')}: {str(e)}")

        try:
            db.commit()
        except Exception as e:
            db.rollback()
            errors.append(f"Database commit error: {str(e)}")

        return saved_count, errors
