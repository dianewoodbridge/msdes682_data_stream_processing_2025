import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Configuration for Confluent Cloud
try:
    conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'group.id': 'user-event-consumer-group',  # A unique ID for the consumer group
        'auto.offset.reset': 'earliest',         # Start from the beginning of the topic if no offset is committed
    }
except KeyError as e:
    print(f"Error: Missing environment variable {e}. Please check your .env file.", file=sys.stderr)
    sys.exit(1)

def run_consumer_microservice(topic_name):
    """
    Consumes messages from a Kafka topic and processes them.
    """
    # Create the Consumer instance
    consumer = Consumer(conf)
    
    print(f"Starting consumer microservice. Consuming from '{topic_name}'...")
    try:
        # Subscribe to the topic(s)
        consumer.subscribe([topic_name])
        
        while True:
            # Poll for messages with a timeout.
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message available within the timeout period
                continue
            if msg.error():
                # Handle Kafka errors
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event - not a real error
                    print(f"Reached end of partition {msg.partition()} for topic {msg.topic()}")
                else:
                    print(f"Consumer error: {msg.error()}", file=sys.stderr)
            else:
                # Message is valid. Process it.
                print(f"Received message from topic '{msg.topic()}' on partition {msg.partition()} at offset {msg.offset()}")
                try:
                    # Decode the message value (JSON string) to an object
                    user_data = json.loads(msg.value().decode('utf-8'))
                    print(f"Key: {msg.key().decode('utf-8')}, Value: {user_data}")
                    
                    #############################################################
                    # TODO                                                      #  
                    # Here you can add your processing logic for the user_data  #
                    #############################################################
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON from message: {e}", file=sys.stderr)
                except Exception as e:
                    print(f"An error occurred while processing the message: {e}", file=sys.stderr)
            
            # To simulate a potential rebalance, pause to allow other consumers to join/leave
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("Consumer microservice stopped by user.")
    finally:
        # Close down the consumer to commit final offsets.
        print("Closing consumer...")
        consumer.close()

if __name__ == '__main__':
    topic_name = 'user.topic'
    run_consumer_microservice(topic_name)
