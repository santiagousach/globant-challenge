from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text, func, and_, extract, case
from typing import List
from ..database import get_db
from ..models import Employee, Department, Job
from ..schemas import HiringMetricsResponse, DepartmentHiringResponse

router = APIRouter()

@router.get("/metrics/hiring-by-quarter", response_model=List[HiringMetricsResponse])
async def get_hiring_by_quarter(db: Session = Depends(get_db)):
    """
    Get number of employees hired for each job and department in 2021 divided by quarter.
    Results ordered alphabetically by department and job.
    """
    try:
        # Define quarter extraction using case statement
        quarter_case = case(
            (extract('month', Employee.datetime).between(1, 3), 'Q1'),
            (extract('month', Employee.datetime).between(4, 6), 'Q2'),
            (extract('month', Employee.datetime).between(7, 9), 'Q3'),
            (extract('month', Employee.datetime).between(10, 12), 'Q4'),
            else_='Other'
        )

        # Query with joins and aggregations
        results = db.query(
            Department.department,
            Job.job,
            func.sum(case((quarter_case == 'Q1', 1), else_=0)).label('Q1'),
            func.sum(case((quarter_case == 'Q2', 1), else_=0)).label('Q2'),
            func.sum(case((quarter_case == 'Q3', 1), else_=0)).label('Q3'),
            func.sum(case((quarter_case == 'Q4', 1), else_=0)).label('Q4')
        ).join(
            Employee, Employee.department_id == Department.id
        ).join(
            Job, Employee.job_id == Job.id
        ).filter(
            extract('year', Employee.datetime) == 2021
        ).group_by(
            Department.department, Job.job
        ).order_by(
            Department.department, Job.job
        ).all()

        return [
            HiringMetricsResponse(
                department=row[0],
                job=row[1],
                Q1=int(row[2] or 0),
                Q2=int(row[3] or 0),
                Q3=int(row[4] or 0),
                Q4=int(row[5] or 0)
            )
            for row in results
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving hiring metrics: {str(e)}")

@router.get("/metrics/departments-above-average", response_model=List[DepartmentHiringResponse])
async def get_departments_above_average(db: Session = Depends(get_db)):
    """
    Get departments that hired more employees than the mean of employees hired in 2021
    for all departments, ordered by number of employees hired (descending).
    """
    try:
        # First, get the total hires per department for 2021
        dept_hires = db.query(
            Department.id,
            Department.department,
            func.count(Employee.id).label('hired_count')
        ).outerjoin(
            Employee, and_(
                Employee.department_id == Department.id,
                extract('year', Employee.datetime) == 2021
            )
        ).group_by(
            Department.id, Department.department
        ).subquery()

        # Calculate the overall average
        avg_result = db.query(
            func.avg(dept_hires.c.hired_count).label('avg_hires')
        ).first()

        if not avg_result or avg_result[0] is None:
            return []

        avg_hires = float(avg_result[0])

        # Get departments above average
        results = db.query(
            dept_hires.c.id,
            dept_hires.c.department,
            dept_hires.c.hired_count.label('hired')
        ).filter(
            dept_hires.c.hired_count > avg_hires
        ).order_by(
            dept_hires.c.hired_count.desc()
        ).all()

        return [
            DepartmentHiringResponse(
                id=int(row[0]),
                department=row[1],
                hired=int(row[2])
            )
            for row in results
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving department metrics: {str(e)}")
