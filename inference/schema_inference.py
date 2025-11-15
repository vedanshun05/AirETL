"""Core schema inference engine
This is where the ML magic happens
"""
import json
from typing import Dict, Any, List

def flatten_json(data: Any, parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """
    Flattens nested JSON structures
    Example: {"a": {"b": 1}} -> {"a.b": 1}
    """
    items = []
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Handle arrays
                for i, item in enumerate(v):
                    items.extend(flatten_json(item, f"{new_key}[{i}]", sep=sep).items())
            else:
                items.append((new_key, v))
    else:
        items.append((parent_key, data))
    return dict(items)

def infer_type(value: Any) -> str:
    """
    Infer the type of a single value
    TODO: Add ML model for smarter inference
    """
    if value is None:
        return "null"
    
    if isinstance(value, bool):
        return "boolean"
    
    if isinstance(value, int):
        return "integer"
    
    if isinstance(value, float):
        return "float"
    
    if isinstance(value, str):
        # Try to detect if string is actually a number
        if value.replace('.', '').replace('-', '').isdigit():
            if '.' in value:
                return "float"
            return "integer"
        
        # Check if it's a boolean string
        if value.lower() in ['true', 'false', 'yes', 'no']:
            return "boolean"
        
        # Check if it's a price
        if value.startswith('$'):
            return "currency"
        
        return "string"
    
    return "unknown"

def calculate_confidence(values: List[Any], inferred_type: str) -> float:
    """
    Calculate confidence score (0.0 to 1.0) for type inference
    Higher score = more confident
    """
    if not values:
        return 0.0
    
    matching = 0
    for v in values:
        if infer_type(v) == inferred_type:
            matching += 1
    
    return matching / len(values)

# Test function
if __name__ == "__main__":
    test_data = {
        "product_id": "123",
        "price": "$99.99",
        "quantity": 5,
        "nested": {"warehouse": "NYC"}
    }
    
    flattened = flatten_json(test_data)
    print("Flattened:", json.dumps(flattened, indent=2))
    
    for key, value in flattened.items():
        t = infer_type(value)
        print(f"{key}: {value} -> {t}")