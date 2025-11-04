import os
import psutil
import time
import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import topic_record_subject_name_strategy
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

load_dotenv()

avro_schema_str = """
    {
        "type": "record",
        "name": "SystemUsage",
        "namespace": "edu.usfca.msdsai",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "cpu_usage", "type": "float"},
            {"name": "cpu_stats", "type":{"type": "array", "items": "float"}},
            {"name": "memory_usage", "type": "float"},
            {"name": "timestamp", "type": "float"}
        ]
    }
"""

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate the delivery status.
    """
    if err is not None:
        raise Exception(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' "
              f"[Partition {msg.partition()}] at Offset {msg.offset()}")

def get_producer():
    """
    Create and return a configured producer (call once and reuse)
    """
    schema_registry_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = 
    
    producer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        
    }
    return 

def produce_system_usage_message(producer,
                                 record_count,
                                 topic="system.usage"):
    for i in range(record_count):
        try:
            message = {
                "id": str(uuid.uuid4()),
                "cpu_usage": psutil.cpu_percent(),
                "cpu_stats": list(psutil.cpu_stats()),
                "memory_usage": psutil.virtual_memory().percent,
                "timestamp": time.time()
            }
            producer.produce(
                topic=topic,
                key=message["id"],
                value=message,
                on_delivery=delivery_report,
            )
            producer.poll()
        except Exception as e:
            print(f"Error producing message {i+1}/{record_count}: {e}")
    producer.flush()

if __name__ == '__main__':
    producer = get_producer()
    produce_system_usage_message(producer, 10)