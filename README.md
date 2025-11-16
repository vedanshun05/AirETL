# AirETL

A Python-based ETL (Extract, Transform, Load) pipeline with Kafka streaming, PostgreSQL storage, and MinIO object storage.

## 🏗️ Architecture & Workflow

AirETL is an intelligent data pipeline that automatically infers schemas from JSON data using machine learning and processes them through a distributed streaming architecture.

### Workflow Overview

```
JSON Data → Producer → Kafka → Consumer → Schema Inference API → Storage (PostgreSQL/MinIO)
                                    ↓
                            Web UI Dashboard (Real-time Monitoring)
```

### How It Works

1. **Data Ingestion** (`producer.py`)
   - Reads JSON files from the `data/` directory
   - Validates and serializes JSON data
   - Publishes records to Kafka topic `raw-data` with unique keys
   - Handles both single objects and arrays of records

2. **Message Streaming** (Kafka)
   - Acts as a distributed message queue
   - Ensures fault-tolerant, scalable data transfer
   - Decouples producers from consumers for better reliability
   - Maintains message order and delivery guarantees

3. **Data Processing** (`consumer.py`)
   - Subscribes to Kafka topic and consumes messages in real-time
   - Batches records for efficient processing
   - Calls the Schema Inference API for each record
   - Displays inferred schemas with confidence scores
   - Can dynamically store data in PostgreSQL based on inferred schema

4. **ML-Powered Schema Inference** (`schema_inference.py` + `api.py`)
   - **Pattern Detection**: Identifies emails, URLs, dates, phone numbers, UUIDs
   - **Type Inference**: Automatically detects data types (string, integer, float, boolean, etc.)
   - **Confidence Scoring**: Assigns confidence levels (0.0-1.0) to each field
   - **Canonical Mapping**: Maps similar fields (e.g., "prod_price", "item_cost") to canonical names
   - **Nested JSON Handling**: Flattens and processes nested/complex JSON structures
   - Exposed via FastAPI REST endpoint for easy integration

5. **Web Interface** (`app.py`)
   - Flask-based dashboard for monitoring and testing
   - Real-time pipeline status and metrics
   - Manual data submission through UI
   - Analytics and visualization of processed data
   - Health checks for all services

### Tech Stack & Usage

| Technology | Purpose | Usage in AirETL |
|------------|---------|-----------------|
| **FastAPI** | High-performance API framework | Serves the ML schema inference API with automatic OpenAPI docs |
| **Uvicorn** | ASGI server | Runs the FastAPI application with hot-reload support |
| **Kafka** | Distributed streaming platform | Message queue for decoupling data ingestion from processing |
| **Zookeeper** | Kafka coordination | Manages Kafka brokers and maintains cluster state |
| **PostgreSQL** | Relational database | Stores structured data with dynamically created tables |
| **MinIO** | S3-compatible object storage | Stores raw JSON files and large datasets |
| **Boto3** | AWS SDK for Python | Interfaces with MinIO for object storage operations |
| **Flask** | Web framework | Powers the monitoring dashboard and UI |
| **Pandas** | Data manipulation | Processes and transforms data efficiently |
| **NumPy** | Numerical computing | Handles numerical operations in schema inference |
| **Genson** | JSON schema generator | Assists in generating JSON schemas |
| **FastText** | Text classification | ML model for intelligent field name mapping |
| **Prometheus Client** | Metrics collection | Exposes pipeline metrics for monitoring |
| **Psycopg2** | PostgreSQL adapter | Enables Python-PostgreSQL connectivity |

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

## 🎯 Key Features

- **Automatic Schema Detection**: ML-powered inference with confidence scores
- **Pattern Recognition**: Identifies emails, URLs, dates, phone numbers, and more
- **Canonical Field Mapping**: Intelligently maps similar field names (e.g., "price", "cost" → "canonical_price")
- **Real-time Processing**: Kafka-based streaming for scalable data ingestion
- **Distributed Architecture**: Decoupled components for reliability and scalability
- **Web Dashboard**: Monitor pipeline health and view analytics in real-time
- **Flexible Storage**: PostgreSQL for structured data, MinIO for object storage

---

Built with ❤️ for intelligent data engineering
