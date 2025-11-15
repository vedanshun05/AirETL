"""
Kafka Producer - Reads JSON files and pushes to Kafka
"""
import json
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import os

class SchemaProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='raw-data'):
        """Initialize Kafka producer"""
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        print(f"✅ Producer connected to Kafka at {bootstrap_servers}")
        print(f"📤 Publishing to topic: {topic}")
    
    def send_record(self, record, key=None):
        """Send a single record to Kafka"""
        try:
            future = self.producer.send(self.topic, key=key, value=record)
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            return {
                'success': True,
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
        except KafkaError as e:
            print(f"❌ Failed to send record: {e}")
            return {'success': False, 'error': str(e)}
    
    def send_from_file(self, filepath):
        """Read JSON file and send records to Kafka"""
        if not os.path.exists(filepath):
            print(f"❌ File not found: {filepath}")
            return
        
        print(f"\n📂 Reading file: {filepath}")
        
        with open(filepath, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON: {e}")
                return
        
        # Handle both single objects and arrays
        records = data if isinstance(data, list) else [data]
        
        print(f"📊 Found {len(records)} records")
        
        success_count = 0
        for i, record in enumerate(records):
            # Use record index as key
            key = f"record-{i}"
            result = self.send_record(record, key=key)
            
            if result['success']:
                success_count += 1
                print(f"  ✓ Record {i}: offset {result['offset']} in partition {result['partition']}")
            else:
                print(f"  ✗ Record {i}: {result['error']}")
            
            # Small delay to avoid overwhelming Kafka
            time.sleep(0.1)
        
        print(f"\n✅ Sent {success_count}/{len(records)} records successfully")
    
    def close(self):
        """Close producer connection"""
        self.producer.flush()
        self.producer.close()
        print("🔌 Producer closed")


def main():
    """Main function to run producer from command line"""
    if len(sys.argv) < 2:
        print("Usage: python producer.py <json_file>")
        print("Example: python producer.py ../data/sample1.json")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    # Create producer
    producer = SchemaProducer()
    
    try:
        # Send data from file
        producer.send_from_file(filepath)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()