import os

from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv

# Schema Registry configuration
load_dotenv()
schema_registry_conf = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info':f'{os.getenv('SR_API_KEY')}:{os.getenv('SR_API_SECRET')}'
}

# Avro Schema in JSON
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
subject_name = "system.usage-edu.usfca.msdsai.SystemUsage"

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Register the Avro schema
avro_schema = Schema(avro_schema_str, schema_type="AVRO")
schema_id = schema_registry_client.register_schema(subject_name, avro_schema)

print(f"Schema registered with ID: {schema_id}")