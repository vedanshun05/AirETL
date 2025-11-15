"""
Flask Demo Web UI for Schema Inference
"""
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
import requests
import json
import os
from datetime import datetime
import sys

# Add parent directory to path to import pipeline modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.producer import SchemaProducer
from pipeline.storage import DataStorage

app = Flask(__name__)
app.secret_key = 'hackathon-secret-key-2024'

# Configuration
INFERENCE_API = os.getenv('INFERENCE_API', 'http://localhost:8001')
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize storage
storage = DataStorage(storage_type='local')


@app.route('/')
def index():
    """Home page"""
    return render_template('index.html')


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    """Upload JSON file and infer schema"""
    if request.method == 'GET':
        return render_template('upload.html')
    
    if 'file' not in request.files:
        flash('No file selected', 'error')
        return redirect(url_for('upload'))
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('upload'))
    
    if not file.filename.endswith('.json'):
        flash('Only JSON files are allowed', 'error')
        return redirect(url_for('upload'))
    
    try:
        # Read and parse JSON
        content = file.read().decode('utf-8')
        data = json.loads(content)
        
        # Handle both single objects and arrays
        records = data if isinstance(data, list) else [data]
        
        # Call inference API
        response = requests.post(
            f"{INFERENCE_API}/infer",
            json={"data": records},
            timeout=10
        )
        
        if response.status_code != 200:
            flash(f'Inference API error: {response.status_code}', 'error')
            return redirect(url_for('upload'))
        
        result = response.json()
        
        # Save to storage
        record_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        storage.save_record(records, result, record_id=record_id)
        
        # Prepare data for display
        fields = result.get('fields', {})
        canonical_mapping = result.get('canonical_mapping', {})
        total_records = result.get('total_records', len(records))
        
        return render_template(
            'results.html',
            filename=file.filename,
            fields=fields,
            canonical_mapping=canonical_mapping,
            total_records=total_records,
            raw_data=records[:3]  # Show first 3 records
        )
        
    except json.JSONDecodeError as e:
        flash(f'Invalid JSON file: {str(e)}', 'error')
        return redirect(url_for('upload'))
    except requests.exceptions.RequestException as e:
        flash(f'Failed to connect to inference API: {str(e)}', 'error')
        return redirect(url_for('upload'))
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('upload'))


@app.route('/api/infer', methods=['POST'])
def api_infer():
    """API endpoint for schema inference"""
    try:
        data = request.get_json()
        
        if not data or 'data' not in data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Call inference API
        response = requests.post(
            f"{INFERENCE_API}/infer",
            json=data,
            timeout=10
        )
        
        return jsonify(response.json()), response.status_code
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/pipeline')
def pipeline_view():
    """View pipeline and send to Kafka"""
    return render_template('pipeline.html')


@app.route('/api/send-to-kafka', methods=['POST'])
def send_to_kafka():
    """Send data to Kafka pipeline"""
    try:
        data = request.get_json()
        records = data.get('records', [])
        
        if not records:
            return jsonify({'error': 'No records provided'}), 400
        
        # Initialize producer
        producer = SchemaProducer()
        
        results = []
        for i, record in enumerate(records):
            result = producer.send_record(record, key=f"web-{i}")
            results.append(result)
        
        producer.close()
        
        success_count = sum(1 for r in results if r.get('success'))
        
        return jsonify({
            'success': True,
            'total': len(records),
            'sent': success_count,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/history')
def history():
    """View processing history"""
    records = storage.list_records(limit=20)
    return render_template('history.html', records=records)


@app.route('/health')
def health():
    """Health check"""
    # Check if inference API is accessible
    try:
        response = requests.get(f"{INFERENCE_API}/health", timeout=2)
        api_status = 'healthy' if response.status_code == 200 else 'unhealthy'
    except:
        api_status = 'unreachable'
    
    return jsonify({
        'status': 'healthy',
        'inference_api': api_status,
        'timestamp': datetime.utcnow().isoformat()
    })


if __name__ == '__main__':
    print("🚀 Starting Schema Inference Demo UI")
    print(f"📍 Inference API: {INFERENCE_API}")
    print(f"🌐 Access at: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)