from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from .database import engine, Base
from .routes.upload import router as upload_router
from .routes.metrics import router as metrics_router

# Create database tables
Base.metadata.create_all(bind=engine)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Globant Data Engineering Challenge API",
    description="REST API for processing employee data and generating hiring metrics",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(
    upload_router,
    prefix="/api/v1",
    tags=["Upload"]
)

app.include_router(
    metrics_router,
    prefix="/api/v1",
    tags=["Metrics"]
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Globant Data Engineering Challenge API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
