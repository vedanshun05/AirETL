"""
Enhanced Schema Inference Engine with Pattern Detection
"""
import json
import re
from typing import Dict, Any, List, Tuple
from datetime import datetime

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
                # Handle arrays - flatten each element
                for i, item in enumerate(v):
                    if isinstance(item, (dict, list)):
                        items.extend(flatten_json(item, f"{new_key}[{i}]", sep=sep).items())
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            items.extend(flatten_json(item, f"{parent_key}[{i}]", sep=sep).items())
    else:
        items.append((parent_key, data))
    
    return dict(items)


def detect_patterns(value: str) -> List[str]:
    """
    Detect specific patterns in string values
    Returns list of detected patterns
    """
    patterns = []
    
    # Email pattern
    if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value):
        patterns.append('email')
    
    # URL pattern
    if re.match(r'^https?://', value):
        patterns.append('url')
    
    # Date patterns
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
        r'^\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
    ]
    for pattern in date_patterns:
        if re.match(pattern, value):
            patterns.append('date')
            break
    
    # Currency pattern
    if re.match(r'^[\$£€¥]\s*\d+\.?\d*$', value) or re.match(r'^\d+\.?\d*\s*[\$£€¥]$', value):
        patterns.append('currency')
    
    # Phone pattern
    if re.match(r'^[\+]?[(]?\d{1,4}[)]?[-\s\.]?\d{1,4}[-\s\.]?\d{1,9}$', value):
        patterns.append('phone')
    
    # UUID pattern
    if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', value.lower()):
        patterns.append('uuid')
    
    return patterns


def infer_type(value: Any) -> Tuple[str, float]:
    """
    Infer the type of a single value with confidence score
    Returns: (type, confidence)
    """
    if value is None:
        return ("null", 1.0)
    
    # Boolean
    if isinstance(value, bool):
        return ("boolean", 1.0)
    
    # Integer
    if isinstance(value, int) and not isinstance(value, bool):
        return ("integer", 1.0)
    
    # Float
    if isinstance(value, float):
        return ("float", 1.0)
    
    # String processing
    if isinstance(value, str):
        # Check for patterns first
        patterns = detect_patterns(value)
        if patterns:
            # Return most specific pattern with high confidence
            return (patterns[0], 0.95)
        
        # Empty string
        if not value.strip():
            return ("string", 0.5)
        
        # Boolean-like strings
        if value.lower() in ['true', 'false', 'yes', 'no', 't', 'f', 'y', 'n']:
            return ("boolean", 0.85)
        
        # Numeric strings
        clean_value = value.replace('$', '').replace('£', '').replace('€', '').replace(',', '').strip()
        
        # Try integer
        if clean_value.lstrip('-').isdigit():
            return ("integer", 0.8)
        
        # Try float
        try:
            float(clean_value)
            if '.' in clean_value:
                return ("float", 0.8)
            return ("integer", 0.75)
        except ValueError:
            pass
        
        # Default string
        return ("string", 1.0)
    
    # Array
    if isinstance(value, list):
        return ("array", 1.0)
    
    # Object
    if isinstance(value, dict):
        return ("object", 1.0)
    
    return ("unknown", 0.0)


def calculate_confidence(values: List[Any], inferred_type: str) -> float:
    """
    Calculate confidence score (0.0 to 1.0) for type inference across multiple values
    """
    if not values:
        return 0.0
    
    type_scores = []
    
    for v in values:
        detected_type, confidence = infer_type(v)
        if detected_type == inferred_type:
            type_scores.append(confidence)
        else:
            type_scores.append(0.0)
    
    # Average confidence across all values
    avg_confidence = sum(type_scores) / len(type_scores)
    
    return round(avg_confidence, 2)


def infer_field_alias(field_name: str) -> str:
    """
    Detect common field aliases and map to canonical names
    This is the "pricing lens" innovation
    """
    field_lower = field_name.lower()
    
    # Price-related fields
    if any(x in field_lower for x in ['price', 'cost', 'amount', 'total', 'value']):
        return 'canonical_price'
    
    # Quantity fields
    if any(x in field_lower for x in ['qty', 'quantity', 'count', 'num']):
        return 'canonical_quantity'
    
    # ID fields
    if any(x in field_lower for x in ['id', 'identifier', 'key']):
        return 'canonical_id'
    
    # Name fields
    if any(x in field_lower for x in ['name', 'title', 'label']):
        return 'canonical_name'
    
    # Date fields
    if any(x in field_lower for x in ['date', 'time', 'timestamp', 'created', 'updated']):
        return 'canonical_timestamp'
    
    return field_name


def infer_schema(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Main function: infer schema from multiple JSON records
    Returns schema with types, confidence scores, and field mappings
    """
    if not data:
        return {"error": "No data provided"}
    
    # Collect all field values
    field_values = {}
    for record in data:
        flattened = flatten_json(record)
        for key, value in flattened.items():
            if key not in field_values:
                field_values[key] = []
            field_values[key].append(value)
    
    # Infer type for each field
    schema = {}
    canonical_mapping = {}
    
    for field, values in field_values.items():
        # Get all types detected
        type_detections = [infer_type(v) for v in values]
        types_only = [t[0] for t in type_detections]
        
        # Find most common type
        type_counts = {}
        for t in types_only:
            type_counts[t] = type_counts.get(t, 0) + 1
        
        inferred_type = max(type_counts, key=type_counts.get)
        confidence = calculate_confidence(values, inferred_type)
        
        # Detect canonical field mapping
        canonical = infer_field_alias(field)
        if canonical != field:
            canonical_mapping[field] = canonical
        
        schema[field] = {
            "type": inferred_type,
            "confidence": confidence,
            "sample_values": values[:3],  # First 3 examples
            "total_values": len(values),
            "null_count": sum(1 for v in values if v is None)
        }
        
        if canonical != field:
            schema[field]["canonical_name"] = canonical
    
    return {
        "fields": schema,
        "canonical_mapping": canonical_mapping,
        "total_records": len(data)
    }


# Test the code
if __name__ == "__main__":
    print("=" * 60)
    print("Schema Inference Engine Test")
    print("=" * 60)
    
    # Test 1: Simple data
    test_data_1 = [
        {"product_id": "123", "price": "$99.99", "quantity": 5},
        {"product_id": "456", "price": "$150.50", "quantity": 10}
    ]
    
    print("\n📋 Test 1: Simple E-commerce Data")
    result = infer_schema(test_data_1)
    print(json.dumps(result, indent=2))
    
    # Test 2: Messy data with conflicts
    test_data_2 = [
        {"user_id": "abc123", "age": 25, "score": "95.5", "active": "true"},
        {"user_id": "def456", "age": "30", "score": 88, "active": True},
        {"user_id": "ghi789", "age": 35, "score": "92", "active": "yes"}
    ]
    
    print("\n📋 Test 2: Messy Data with Type Conflicts")
    result = infer_schema(test_data_2)
    print(json.dumps(result, indent=2))
    
    # Test 3: Pattern detection
    test_data_3 = [
        {
            "email": "user@example.com",
            "website": "https://example.com",
            "signup_date": "2024-01-15",
            "phone": "+1-555-123-4567"
        }
    ]
    
    print("\n📋 Test 3: Pattern Detection")
    result = infer_schema(test_data_3)
    print(json.dumps(result, indent=2))
    
    print("\n✅ All tests complete!")