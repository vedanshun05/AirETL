"""
Flask Web UI for AirETL Pipeline
Enhanced with real-time features
"""
from flask import Flask, render_template, request, jsonify
import os
import json
from datetime import datetime
import glob
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

# Configuration
STORAGE_DIR = os.path.join(os.path.dirname(__file__), 'storage')
INFERENCE_API_URL = "http://localhost:8001"

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)

@app.route('/')
def upload():
    """Enhanced upload interface"""
    return render_template('index.html')

@app.route('/results')
def results():
    """Show all processed results"""
    try:
        # Read all JSON files from storage
        result_files = glob.glob(os.path.join(STORAGE_DIR, 'record_*.json'))
        results = []
        
        for file_path in sorted(result_files, reverse=True)[:50]:  # Last 50 results
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    results.append({
                        'filename': os.path.basename(file_path),
                        'timestamp': data.get('timestamp', datetime.now().isoformat()),
                        'record_count': len(data.get('data', [])) if isinstance(data.get('data'), list) else 1,
                        'schema': data.get('schema', {}),
                        'confidence': data.get('confidence', 0)
                    })
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON in {file_path}: {e}")
                continue
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")
                continue
        
        return render_template('results.html', results=results)
    
    except Exception as e:
        print(f"❌ Route error: {e}")
        return render_template('results.html', results=[], error=str(e))

@app.route('/pipeline')
def pipeline():
    """Show pipeline status"""
    try:
        # Check Kafka status
        kafka_status = check_kafka_status()
        
        # Check API status
        api_status = check_api_status()
        
        # Get recent processing stats
        stats = get_processing_stats()
        
        return render_template('pipeline.html', 
                             kafka_status=kafka_status,
                             api_status=api_status,
                             stats=stats)
    
    except Exception as e:
        print(f"❌ Pipeline route error: {e}")
        return render_template('pipeline.html', 
                             kafka_status=False,
                             api_status=False,
                             stats={
                                 'total_files': 0,
                                 'total_records': 0,
                                 'avg_records_per_file': 0
                             },
                             error=str(e))
    
@app.route('/api/pipeline/metrics')
def pipeline_metrics():
    """Get real-time pipeline metrics"""
    try:
        stats = get_processing_stats()
        
        # Calculate throughput (simplified)
        import time
        current_time = time.time()
        
        return jsonify({
            'success': True,
            'metrics': {
                'total_files': stats['total_files'],
                'total_records': stats['total_records'],
                'avg_per_file': stats['avg_records_per_file'],
                'throughput': stats.get('throughput', 0),
                'timestamp': datetime.now().isoformat()
            },
            'status': {
                'kafka': check_kafka_status(),
                'api': check_api_status(),
                'storage': os.path.exists(STORAGE_DIR)
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    


@app.route('/analytics')
def analytics():
    """Analytics dashboard"""
    try:
        stats = calculate_analytics()
        return render_template('analytics.html', stats=stats)
    except Exception as e:
        return f"Error loading analytics: {str(e)}", 500

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        # Check API
        api_healthy = check_api_status()
        
        # Check Kafka (simplified)
        kafka_healthy = True  # You can add actual Kafka health check
        
        return jsonify({
            'status': 'healthy' if (api_healthy and kafka_healthy) else 'degraded',
            'api': 'healthy' if api_healthy else 'unhealthy',
            'kafka': 'healthy' if kafka_healthy else 'unhealthy',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

@app.route('/send_to_kafka', methods=['POST'])
def send_to_kafka():
    """Send schema to Kafka pipeline"""
    try:
        data = request.get_json()
        
        # Here you would integrate with your producer
        # For now, we'll save to storage
        filename = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(STORAGE_DIR, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return jsonify({
            'success': True,
            'message': 'Data queued for processing',
            'filename': filename
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/inference', methods=['POST'])
def inference_proxy():
    """Proxy requests to inference API"""
    try:
        data = request.get_json()
        
        response = requests.post(
            f"{INFERENCE_API_URL}/infer",
            json=data,
            timeout=10
        )
        
        return jsonify(response.json())
    except requests.exceptions.ConnectionError:
        return jsonify({
            'error': 'Inference API is not running',
            'message': 'Please start the inference API: cd inference && uvicorn api:app --port 8001'
        }), 503
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/storage/<filename>')
def serve_storage_file(filename):
    """Serve storage files for viewing"""
    try:
        filepath = os.path.join(STORAGE_DIR, filename)
        if not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Helper functions

def check_kafka_status():
    """Check if Kafka is running"""
    try:
        # Simple check - you can enhance this
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        return result == 0
    except:
        return False

def check_api_status():
    """Check if inference API is running"""
    try:
        response = requests.get(f"{INFERENCE_API_URL}/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def get_processing_stats():
    """Get processing statistics"""
    try:
        result_files = glob.glob(os.path.join(STORAGE_DIR, 'record_*.json'))
        
        total_records = 0
        total_files = len(result_files)
        
        for file_path in result_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    total_records += len(data.get('data', []))
            except:
                continue
        
        return {
            'total_files': total_files,
            'total_records': total_records,
            'avg_records_per_file': total_records / total_files if total_files > 0 else 0
        }
    except:
        return {
            'total_files': 0,
            'total_records': 0,
            'avg_records_per_file': 0
        }
    


def calculate_analytics():
    """Calculate analytics data"""
    try:
        result_files = glob.glob(os.path.join(STORAGE_DIR, 'record_*.json'))
        
        total_fields = 0
        total_schemas = 0
        type_distribution = {}
        confidence_scores = []
        
        for file_path in result_files[:100]:  # Analyze last 100 files
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    schema = data.get('schema', {})
                    
                    total_schemas += 1
                    total_fields += len(schema.get('fields', []))
                    
                    # Type distribution
                    for field in schema.get('fields', []):
                        field_type = field.get('inferred_type', 'unknown')
                        type_distribution[field_type] = type_distribution.get(field_type, 0) + 1
                        
                        # Confidence scores
                        confidence = field.get('confidence', 0)
                        confidence_scores.append(confidence)
            except:
                continue
        
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        
        return {
            'total_schemas': total_schemas,
            'total_fields': total_fields,
            'avg_fields_per_schema': total_fields / total_schemas if total_schemas > 0 else 0,
            'avg_confidence': avg_confidence,
            'type_distribution': type_distribution,
            'field_growth_trend': []  # You can add trend calculation
        }
    except Exception as e:
        print(f"Analytics error: {e}")
        return {
            'total_schemas': 0,
            'total_fields': 0,
            'avg_fields_per_schema': 0,
            'avg_confidence': 0,
            'type_distribution': {},
            'field_growth_trend': []
        }

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('500.html'), 500

if __name__ == '__main__':
    print("🚀 Starting AirETL Web UI...")
    print(f"📁 Storage directory: {STORAGE_DIR}")
    print(f"🔗 Inference API URL: {INFERENCE_API_URL}")
    print("📊 Dashboard: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)