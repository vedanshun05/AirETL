"""
FastAPI service for schema inference
Run with: uvicorn api:app --reload --port 8001
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List
import schema_inference

app = FastAPI(
    title="Schema Inference API",
    description="ML-powered schema inference with confidence scoring",
    version="1.0.0"
)

# Enable CORS for web UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class InferRequest(BaseModel):
    data: List[Dict[str, Any]]

class InferResponse(BaseModel):
    fields: Dict[str, Any]
    canonical_mapping: Dict[str, str]
    total_records: int

@app.post("/infer", response_model=InferResponse)
async def infer_schema(request: InferRequest):
    """
    Infer schema from JSON data with confidence scores
    """
    if not request.data:
        raise HTTPException(status_code=400, detail="No data provided")
    
    try:
        result = schema_inference.infer_schema(request.data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "schema-inference",
        "version": "1.0.0"
    }

@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "message": "Schema Inference API",
        "endpoints": {
            "POST /infer": "Infer schema from JSON data",
            "GET /health": "Health check",
            "GET /docs": "API documentation"
        }
    }