
from faker import Faker
import os
import psutil
import random
import time


from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import topic_subject_name_strategy
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

load_dotenv()

avro_schema_str = """
    {
        "type": "record",
        "name": "SystemUser",
        "namespace": "edu.usfca.msdsai",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"}
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
        print(f"Message delivered to topic '{msg.topic()} - id: {int(msg.key())}' "
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
        conf={'subject.name.strategy': topic_subject_name_strategy}
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
                                 topic="system.user"):
    admin_client = AdminClient({'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
    })
    topics = admin_client.list_topics(timeout=10)
    if topic not in topics.topics:
        admin_client.create_topics([NewTopic(topic)])
    fake = Faker()  
    for i in range(record_count):
        try:
            id = random.randint(1, 5)
            message = {
                "id": str(id),
                "name": fake.name()
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
        time.sleep(1)
    producer.flush()

if __name__ == '__main__':
    producer = get_producer()
    produce_system_usage_message(producer, 10)