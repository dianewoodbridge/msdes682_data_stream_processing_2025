import json
import os
import socket
import sys
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
# Configuration for your Kafka cluster.
# For local Kafka using Confluent Platform, use default ports.
# conf = {
#     'bootstrap.servers': 'localhost:9092', # Change for Confluent Cloud
#     'client.id': socket.gethostname()
# }

# For Confluent Cloud, provide your specific connection details.
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

def create_topic_if_not_exists(admin_client, topic_name, num_partitions=1):
    """
    Creates a Kafka topic if it does not already exist.
    Waits for the creation to finish to prevent a race condition.
    """
    topic_list = None
    try:
        topic_list = admin_client.list_topics(timeout=10).topics
    except KafkaException as e:
        print(f"Failed to list topics due to KafkaException: {e}", file=sys.stderr)
        return

    if topic_name not in topic_list:
        print(f"Topic '{topic_name}' not found. Creating topic...")
        
        # Confluent Cloud manages the replication factor, so we don't set it.
        new_topic = NewTopic(topic_name, num_partitions=num_partitions)
        
        try:
            admin_client.create_topics([new_topic])

        except Exception as e:
            # Handle cases where topic creation fails, e.g., if it already exists
            print(f"Failed to create topic '{topic}': {e}", file=sys.stderr)
    else:
        print(f"Topic '{topic_name}' already exists.")


def delivery_report(err, msg):
    """
    Called once for each message produced to indicate the delivery status.
    """
    if err is not None:
        print(f"Message delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Message delivered to topic '{msg.topic()}' "
              f"[Partition {msg.partition()}] at Offset {msg.offset()}")

def run_producer_microservice(topic_name):
    """
    Generates and produces user event data to Kafka.
    """    
    # Use AdminClient to ensure the topic exists before producing
    admin_client = AdminClient(conf)
    create_topic_if_not_exists(admin_client, topic_name)

    # Create the producer instance
    producer = Producer(conf)
    
    # Initialize data generator
    fake = Faker()
    print(f"Starting producer microservice. Sending data to '{topic_name}'...")
    try:
        while True:
            # Generate mock user data
            user_data = {
                'id': fake.uuid4(),
                'name': fake.name(),
                'email': fake.email(),
                'timestamp': time.time()
            }
            
            # Serialize the data to a JSON string
            json_data = json.dumps(user_data)
            
            # Produce the message to the Kafka topic
            # `on_delivery` specifies the callback function.
            producer.produce(
                topic=topic_name,
                key=str(user_data['id']),
                value=json_data.encode('utf-8'),
                on_delivery=delivery_report
            )
            
            # Flush any outstanding or buffered messages to the Kafka broker.
            producer.poll(0)
            
            # Pause for a moment to simulate a real-time stream
            time.sleep(1)

    except KeyboardInterrupt:
        print("Producer microservice stopped by user.")
    finally:
        # Wait for any outstanding messages to be delivered and delivery reports received.
        producer.flush()

if __name__ == '__main__':
    topic_name = 'user.topic.v2'
    run_producer_microservice(topic_name)
