from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class DepartmentBase(BaseModel):
    id: int
    department: str

class DepartmentCreate(BaseModel):
    department: str

class JobBase(BaseModel):
    id: int
    job: str

class JobCreate(BaseModel):
    job: str

class EmployeeBase(BaseModel):
    id: int
    name: str
    datetime: datetime
    department_id: Optional[int] = None
    job_id: Optional[int] = None

class EmployeeCreate(BaseModel):
    name: str
    datetime: str  # ISO format string
    department_id: Optional[int] = None
    job_id: Optional[int] = None

class EmployeeResponse(EmployeeBase):
    department: Optional[str] = None
    job: Optional[str] = None

    class Config:
        from_attributes = True

class BatchUploadResponse(BaseModel):
    message: str
    processed_rows: int
    errors: List[str] = []

class HiringMetricsResponse(BaseModel):
    department: str
    job: str
    Q1: int
    Q2: int
    Q3: int
    Q4: int

class DepartmentHiringResponse(BaseModel):
    id: int
    department: str
    hired: int
