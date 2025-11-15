"""
FastAPI service for schema inference
Run with: uvicorn inference.api:app --reload --port 8001
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import schema_inference

app = FastAPI(title="Schema Inference API")

class InferRequest(BaseModel):
    data: List[Dict[str, Any]]

class InferResponse(BaseModel):
    fields: Dict[str, Dict[str, Any]]

@app.post("/infer", response_model=InferResponse)
async def infer_schema(request: InferRequest):
    """
    Infer schema from JSON data with confidence scores
    """
    if not request.data:
        raise HTTPException(status_code=400, detail="No data provided")
    
    # Collect all fields and their values
    field_values = {}
    for record in request.data:
        flattened = schema_inference.flatten_json(record)
        for key, value in flattened.items():
            if key not in field_values:
                field_values[key] = []
            field_values[key].append(value)
    
    # Infer types and confidence for each field
    result = {}
    for field, values in field_values.items():
        # Get most common type
        type_counts = {}
        for v in values:
            t = schema_inference.infer_type(v)
            type_counts[t] = type_counts.get(t, 0) + 1
        
        inferred_type = max(type_counts, key=type_counts.get)
        confidence = schema_inference.calculate_confidence(values, inferred_type)
        
        result[field] = {
            "type": inferred_type,
            "confidence": round(confidence, 2),
            "sample_values": values[:3]  # First 3 examples
        }
    
    return InferResponse(fields=result)

@app.get("/health")
async def health():
    return {"status": "healthy"}