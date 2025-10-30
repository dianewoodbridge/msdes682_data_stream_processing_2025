import json
import os
import psutil
import socket
import sys
import time
import uuid

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

load_dotenv()

# 1. Create a FastAPI App Instance
app = FastAPI()

try:
    conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'client.id': socket.gethostname(),
    }
except KeyError as e:
    print(f"Error: Missing environment variable {e}. Please check your .env file.", file=sys.stderr)
    sys.exit(1)

class DataGenerator(BaseModel):
    """
    Create a Pydantic model to validate incoming request data, 
    where num_data is the number of data points to produce (default: 10),
    and topic_name is the Kafka topic to produce to.
    """
    # 3. Define Pydantic Model
    num_data: int = 10
    topic_name: str

class RequestResponse(BaseModel):
    """
    Model for API response messages : Limiting to status and message fields only.
    """
    # 4. Define Response Model
    status: str
    message: str

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate the delivery status.
    """
    if err is not None:
        print(f"Message delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Message delivered to topic '{msg.topic()}' "
              f"[Partition {msg.partition()}] at Offset {msg.offset()}")

@app.post("/produce/", response_model=RequestResponse)
def produce_data(data: DataGenerator):
    """
    Produces the num_data number of system usage data 
    to the specified Kafka topic (topic_name).
    """
    # 2. Define a function and associate with a route.
    topic_name = data.topic_name
    num_data = data.num_data

    if not ensure_topic_exists(topic_name):
        return {"status": "error", 
                "message": f"Could not verify or create topic '{topic_name}'."}

    producer = Producer(conf)
    for _ in range(num_data):
        message = {
            "id": str(uuid.uuid4()),
            "cpu_usage": psutil.cpu_percent(),
            "cpu_stats": psutil.cpu_stats(),
            "memory_usage": psutil.virtual_memory().percent,
            "timestamp": time.time(),
        }

        producer.produce(
            topic=topic_name,
            key=str(message["id"]),
            value=json.dumps(message),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.5)

    producer.flush()
    return {"status": "success",
            "message": f"Produced {num_data} messages to topic '{topic_name}'."}   


def ensure_topic_exists(topic_name: str):
    """
    A helper function to check if a topic exists,
    and create it if it does not.
    """
    admin_client = AdminClient(conf)
    try:
        topics = admin_client.list_topics(timeout=10).topics
    except KafkaException as e:
        print(f"Failed to list topics: {e}", file=sys.stderr)
        return False

    if topic_name not in topics:
        print(f"Topic '{topic_name}' not found. Creating it...")
        new_topic = NewTopic(topic_name)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()  # wait for the topic creation result
                print(f"Topic '{topic}' created successfully.")
                return True
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")
                return False
    else:
        print(f"Topic '{topic_name}' already exists.")
        return True