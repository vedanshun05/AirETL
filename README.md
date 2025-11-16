# AirETL

A Python-based ETL (Extract, Transform, Load) pipeline with Kafka streaming, PostgreSQL storage, and MinIO object storage.

## Prerequisites

- Python 3.8+
- Docker & Docker Compose

## Installation

1. Clone the repository:
```bash
git clone https://github.com/vedanshun05/AirETL.git
cd AirETL
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Required Packages

The project uses the following main libraries:
- **FastAPI** (0.104.1) - Web framework
- **Uvicorn** (0.24.0) - ASGI server
- **Kafka-Python** (2.0.2) - Kafka messaging
- **Boto3** (1.29.0) - AWS S3/MinIO client
- **Psycopg2-Binary** (2.9.9) - PostgreSQL adapter
- **Flask** (3.0.0) - Web framework
- **Pandas** (2.1.3) - Data processing
- **NumPy** (1.26.2) - Numerical computing
- **Genson** (1.2.2) - JSON schema generator
- **FastText-Wheel** (0.9.2) - Text classification
- **Prometheus-Client** (0.19.0) - Metrics monitoring
- **Requests** (2.31.0) - HTTP library

## How to Start

### Step 1: Start Infrastructure Services

Start Kafka, PostgreSQL, and MinIO using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- **Zookeeper** on port 2181
- **Kafka** on port 9092
- **PostgreSQL** on port 5432
  - User: `hackathon`
  - Password: `hackathon123`
  - Database: `schemas`
- **MinIO** on ports 9000 (API) and 9001 (Console)
  - User: `minioadmin`
  - Password: `minioadmin`

### Step 2: Verify Services

Check that all services are running:

```bash
docker-compose ps
```

All services should show status as "Up".

### Step 3: Start the Inference API

In a new terminal, start the FastAPI inference service:

```bash
cd inference
uvicorn api:app --reload --port 8001
```

The API will be available at http://localhost:8001

### Step 4: Start the Pipeline Consumer

In another terminal, start the Kafka consumer:

```bash
cd pipeline
python consumer.py
```

This will start consuming messages from Kafka and processing them through the ETL pipeline.

### Step 5: Start the Demo Application

In another terminal, start the demo Flask application:

```bash
cd demo
python app.py
```

This will start the demo interface for interacting with the AirETL system.

## How to Test the Pipeline

### (a) Through Web UI

Navigate to the demo application URL (typically http://localhost:5000) in your browser. The interface is self-explanatory and allows you to interact with the pipeline visually.

### (b) Through CLI

Navigate to the `pipeline` directory for CLI testing:

```bash
cd pipeline
```

#### Test with a Specific Sample File

To test the pipeline with a specific JSON sample:

```bash
python producer.py ../data/sample1.json
```

You can replace `sample1.json` with any other sample file from the `data/` directory.

#### Test with All Samples

To run tests with all available samples:

```bash
python test_pipeline.py
```

This will process all sample files through the pipeline.

## Sample JSON Types for Testing

The pipeline supports various JSON data structures. Here are 5 types of sample JSON files you can use:

### 1. User Profile Data (sample1.json)
```json
{
  "user_id": "12345",
  "name": "John Doe",
  "email": "john.doe@example.com",
  "age": 30,
  "registration_date": "2024-01-15"
}
```

### 2. E-commerce Transaction (sample2.json)
```json
{
  "transaction_id": "TXN-2024-001",
  "customer_id": "CUST-5678",
  "items": [
    {"product_id": "PROD-100", "quantity": 2, "price": 29.99},
    {"product_id": "PROD-200", "quantity": 1, "price": 49.99}
  ],
  "total_amount": 109.97,
  "timestamp": "2024-01-16T10:30:00Z"
}
```

### 3. IoT Sensor Data (sample3.json)
```json
{
  "sensor_id": "SENSOR-001",
  "location": "Building A - Floor 3",
  "temperature": 22.5,
  "humidity": 45.2,
  "pressure": 1013.25,
  "timestamp": "2024-01-16T14:45:30Z"
}
```

### 4. Social Media Post (sample4.json)
```json
{
  "post_id": "POST-98765",
  "user": "alice_smith",
  "content": "Great day for data engineering!",
  "likes": 150,
  "comments": 23,
  "tags": ["dataengineering", "etl", "kafka"],
  "created_at": "2024-01-16T09:20:15Z"
}
```

### 5. Log Entry Data (sample5.json)
```json
{
  "log_id": "LOG-2024-456",
  "service": "api-gateway",
  "level": "INFO",
  "message": "Request processed successfully",
  "request_id": "REQ-789",
  "duration_ms": 125,
  "status_code": 200,
  "timestamp": "2024-01-16T15:30:45Z"
}
```

Create these sample files in the `data/` directory to test different data schemas through your ETL pipeline.

## Services Access

- **Demo Application**: http://localhost:5000 (or as configured in app.py)
- **Inference API**: http://localhost:8001
- **Inference API Docs**: http://localhost:8001/docs
- **MinIO Console**: http://localhost:9001
- **Kafka**: localhost:9092
- **PostgreSQL**: localhost:5432

## Running Order Summary

1. `docker-compose up -d` - Start infrastructure
2. `uvicorn api:app --reload --port 8001` - Start inference API (from `inference/` directory)
3. `python consumer.py` - Start pipeline consumer (from `pipeline/` directory)
4. `python app.py` - Start demo application (from `demo/` directory)

## Stopping Services

### Stop Python Applications

Press `Ctrl+C` in each terminal running Python applications.

### Stop Docker Services

Stop all Docker containers:

```bash
docker-compose down
```

To remove volumes as well (this will delete all data):

```bash
docker-compose down -v
```
