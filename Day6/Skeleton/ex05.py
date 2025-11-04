import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv

load_dotenv()

schema_registry_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
}
subject_name = "system.usage-edu.usfca.msdsai.SystemUsage"
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 1. Get latest schema for a subject
schema = 

# 2. Get specific version of a schema
schema = schema_registry_client.get_version(subject_name, version=2)

# 3. Get schema by ID (useful when deserializing)
schema_id = 100002 # UPDATE THIS
schema = schema_registry_client.get_schema(schema_id=schema_id)
print(schema.schema_str)

# 4. List all subjects
subjects = schema_registry_client.get_subjects()
print(f"Available subjects: {subjects}")

# 5. Get all versions for a subject
versions = schema_registry_client.get_versions(subject_name)
print(f"Versions: {versions}")
