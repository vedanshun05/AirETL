import json

def flatten_json(data, parent_key=''):
    """Flattens nested JSON - you'll build this during hackathon"""
    items = []
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key).items())
            else:
                items.append((new_key, v))
    return dict(items)

# Test it works
with open('../data/sample1.json') as f:
    data = json.load(f)
    print(flatten_json(data))