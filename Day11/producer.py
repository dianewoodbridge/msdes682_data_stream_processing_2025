import datetime
import os
import psutil
import random
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

load_dotenv()
avro_schema_str = """
    {
        "type": "record",
        "name": "system_usage_day12_value",
        "namespace": "org.apache.flink.avro.generated.record",
        "fields": [
            {"name": "id", "type": ["null", "string"], "default": null},
            {"name": "cpu_usage", "type": ["null", "float"], "default": null},
            {"name": "cpu_stats", "type": ["null", {"type": "array", "items": ["null", "float"]}], "default": null},
            {"name": "memory_usage", "type": ["null", "float"], "default": null},
            {"name": "timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null}
        ]
    }
"""


def delivery_report(err, msg):
    """
    Called once for each message produced to indicate the delivery status.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"✓ Delivered to '{msg.topic()}' - id: {msg.key()} "
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
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str,
    )
    
    producer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    return SerializingProducer(producer_conf)

def produce_system_usage_message(producer,
                                 record_count,
                                 topic="system.usage.day12"):
    admin_client = AdminClient({
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
    })
    
    topics = admin_client.list_topics(timeout=10)
    if topic not in topics.topics:
        admin_client.create_topics([NewTopic(topic)])
    
    print(f"Producing {record_count} messages to '{topic}'...")
    print(f"Current UTC time: {datetime.datetime.utcnow()}")
    print()
    
    for i in range(record_count):
        try:
            id = random.randint(1, 3)
            
            current_time_ms = int(datetime.datetime.utcnow().timestamp() * 1000)
            
            message = {
                "id": str(id),
                "cpu_usage": float(psutil.cpu_percent()),
                "cpu_stats": [float(x) for x in psutil.cpu_stats()],
                "memory_usage": float(psutil.virtual_memory().percent),
                "timestamp": current_time_ms
            }
            
            if(id == 3):
                time.sleep(4 + random.random())
            producer.produce(
                topic=topic,
                key=message["id"],
                value=message,
                on_delivery=delivery_report,
            )
            producer.poll()
            
            # Show what we're sending
            readable_time = datetime.datetime.utcfromtimestamp(current_time_ms / 1000)
            print(f"  #{i+1}: id={id}, mem={message['memory_usage']:.1f}%, "
                  f"time={readable_time.strftime('%H:%M:%S')}")
            
            # Small delay between messages
            time.sleep(1)
            
        except Exception as e:
            print(f"Error producing message {i+1}/{record_count}: {e}")
            import traceback
            traceback.print_exc()
    
    producer.flush()
    print()
    print(f"✓ Flushed all {record_count} messages")

if __name__ == '__main__':
    producer = get_producer()
    
    print("=" * 80)
    print("PRODUCER: Sending messages with CURRENT timestamps")
    print("=" * 80)
    print()
    
    # Produce messages continuously
    while True:
        produce_system_usage_message(producer, 10)
        print("\nWaiting 5 seconds before next batch...")
        print("-" * 80)
        time.sleep(5)
