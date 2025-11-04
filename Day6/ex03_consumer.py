import os
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import topic_record_subject_name_strategy
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv

load_dotenv()

def consume_system_usage_messages(topic="system.usage",
                                  group_id="system-usage-consumer-group"):
    """
    Consume messages from the system.usage topic
    """
    # Schema Registry configuration
    schema_registry_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }

    # Avro schema (same as producer)
    avro_schema_str = """
        {
            "type": "record",
            "name": "SystemUsage",
            "namespace": "edu.usfca.msdsai",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "cpu_usage", "type": "float"},
                {"name": "cpu_stats", 
                 "type":{"type": "array", "items": "float"}},
                {"name": "memory_usage", "type": "float"},
                {"name": "timestamp", "type": "float"},
                {"name": "system", "type":"string", "default":"Linux"}
            ]
        }
    """

    # Create Schema Registry client
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Create Avro Deserializer
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        avro_schema_str,
        conf={'subject.name.strategy': topic_record_subject_name_strategy}
    )

    # Consumer configuration
    consumer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    # Create Consumer instance
    consumer = DeserializingConsumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    print(f"Consuming messages from topic '{topic}'...")

    try:
        while True:
            try:
                # Poll for messages
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # Get the deserialized message
                system_usage = msg.value()
                key = msg.key()

                # Display the message
                print(f"─" * 80)
                print(f"Message Key: {key}")
                print(f"ID: {system_usage['id']}")
                print(f"CPU Usage: {system_usage['cpu_usage']:.2f}%")
                print(f"CPU Stats: {system_usage['cpu_stats']}")
                print(f"Memory Usage: {system_usage['memory_usage']:.2f}%")
                print(f"Timestamp: {system_usage['timestamp']}")
                print(f"Data : {system_usage}")
                print(f"Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")
                print()
                
            except Exception as e:
                print(f"Error deserializing message at offset {msg.offset() if msg else 'unknown'}: {e}")
                print("Skipping to next message...\n")
                continue

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        # Close down consumer to commit final offsets
        consumer.close()
        print("Consumer closed.")


if __name__ == '__main__':
    consume_system_usage_messages()