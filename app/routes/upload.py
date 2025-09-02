from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.orm import Session
import logging
from ..database import get_db
from ..csv_processor import CSVProcessor
from ..schemas import BatchUploadResponse

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/upload/departments", response_model=BatchUploadResponse)
async def upload_departments_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload departments CSV file"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")

    try:
        content = await file.read()
        departments_data = CSVProcessor.process_departments_csv(content)

        processed_count = 0
        errors = []

        for dept_data in departments_data:
            try:
                from ..models import Department
                # Check if department already exists
                existing = db.query(Department).filter(Department.id == dept_data['id']).first()
                if existing:
                    continue

                department = Department(
                    id=dept_data['id'],
                    department=dept_data['department']
                )
                db.add(department)
                processed_count += 1
            except Exception as e:
                errors.append(f"Error processing department {dept_data.get('id')}: {str(e)}")

        db.commit()
        return BatchUploadResponse(
            message="Departments uploaded successfully",
            processed_rows=processed_count,
            errors=errors
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Error uploading departments: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload/jobs", response_model=BatchUploadResponse)
async def upload_jobs_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload jobs CSV file"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")

    try:
        content = await file.read()
        jobs_data = CSVProcessor.process_jobs_csv(content)

        processed_count = 0
        errors = []

        for job_data in jobs_data:
            try:
                from ..models import Job
                # Check if job already exists
                existing = db.query(Job).filter(Job.id == job_data['id']).first()
                if existing:
                    continue

                job = Job(
                    id=job_data['id'],
                    job=job_data['job']
                )
                db.add(job)
                processed_count += 1
            except Exception as e:
                errors.append(f"Error processing job {job_data.get('id')}: {str(e)}")

        db.commit()
        return BatchUploadResponse(
            message="Jobs uploaded successfully",
            processed_rows=processed_count,
            errors=errors
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Error uploading jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload/employees", response_model=BatchUploadResponse)
async def upload_employees_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload employees CSV file with batch processing"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")

    try:
        content = await file.read()
        employee_batches = CSVProcessor.process_employees_csv(content)

        total_processed = 0
        all_errors = []

        for batch in employee_batches:
            processed, errors = CSVProcessor.save_batch_to_db(db, batch)
            total_processed += processed
            all_errors.extend(errors)

        return BatchUploadResponse(
            message=f"Employees uploaded successfully in {len(employee_batches)} batches",
            processed_rows=total_processed,
            errors=all_errors
        )

    except Exception as e:
        logger.error(f"Error uploading employees: {e}")
        raise HTTPException(status_code=500, detail=str(e))
