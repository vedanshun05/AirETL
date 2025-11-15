"""
Kafka Consumer - Reads from Kafka and processes data
"""
import json
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
import requests

class SchemaConsumer:
    def __init__(self, 
                 bootstrap_servers='localhost:9092',
                 topic='raw-data',
                 group_id='schema-consumer-group',
                 inference_api='http://localhost:8001'):
        """Initialize Kafka consumer"""
        self.topic = topic
        self.inference_api = inference_api
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print(f"✅ Consumer connected to Kafka at {bootstrap_servers}")
        print(f"📥 Consuming from topic: {topic}")
        print(f"🔍 Using inference API: {inference_api}")
        print(f"👥 Consumer group: {group_id}")
        print("\n⏳ Waiting for messages... (Press Ctrl+C to stop)\n")
    
    def infer_schema(self, records):
        """Call inference API to get schema"""
        try:
            response = requests.post(
                f"{self.inference_api}/infer",
                json={"data": records},
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️  Inference API error: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to call inference API: {e}")
            return None
    
    def process_record(self, message):
        """Process a single message"""
        print(f"\n{'='*60}")
        print(f"📨 Received message:")
        print(f"  Topic: {message.topic}")
        print(f"  Partition: {message.partition}")
        print(f"  Offset: {message.offset}")
        print(f"  Key: {message.key.decode('utf-8') if message.key else 'None'}")
        print(f"  Timestamp: {message.timestamp}")
        
        record = message.value
        print(f"\n📄 Record data:")
        print(json.dumps(record, indent=2))
        
        # Call inference API
        print(f"\n🔍 Inferring schema...")
        schema_result = self.infer_schema([record])
        
        if schema_result:
            print(f"\n📊 Inferred Schema:")
            
            # Display fields with confidence scores
            fields = schema_result.get('fields', {})
            for field_name, field_info in fields.items():
                confidence = field_info.get('confidence', 0)
                field_type = field_info.get('type', 'unknown')
                canonical = field_info.get('canonical_name', '')
                
                # Color coding based on confidence
                if confidence >= 0.8:
                    icon = "🟢"  # High confidence
                elif confidence >= 0.5:
                    icon = "🟡"  # Medium confidence
                else:
                    icon = "🔴"  # Low confidence
                
                print(f"  {icon} {field_name}:")
                print(f"      Type: {field_type}")
                print(f"      Confidence: {confidence}")
                if canonical:
                    print(f"      Canonical: {canonical}")
            
            # Display canonical mappings
            canonical_mapping = schema_result.get('canonical_mapping', {})
            if canonical_mapping:
                print(f"\n🔗 Canonical Mappings:")
                for original, canonical in canonical_mapping.items():
                    print(f"  {original} → {canonical}")
            
            print(f"\n✅ Processed successfully!")
        else:
            print(f"❌ Schema inference failed")
        
        print(f"{'='*60}\n")
    
    def consume(self, batch_size=10, timeout_ms=5000):
        """Consume messages from Kafka"""
        try:
            message_count = 0
            
            for message in self.consumer:
                self.process_record(message)
                message_count += 1
                
                # Optional: stop after processing N messages
                # if message_count >= batch_size:
                #     print(f"\n🛑 Processed {message_count} messages, stopping...")
                #     break
                
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close consumer connection"""
        self.consumer.close()
        print("🔌 Consumer closed")


def main():
    """Main function to run consumer from command line"""
    # Create consumer
    consumer = SchemaConsumer()
    
    # Start consuming
    consumer.consume()


if __name__ == "__main__":
    main()