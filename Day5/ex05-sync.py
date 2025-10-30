import os

from confluent_kafka import Consumer
from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()
app = FastAPI()

conf = {
    'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.environ['CONFLUENT_API_KEY'],
    'sasl.password': os.environ['CONFLUENT_API_SECRET'],
    'group.id': 'sync-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['system.usage'])

@app.get("/consume/sync")
def consume_sync():
    """
    Blocking version 
    This will block other requests to this endpoint
    until the loop breaks.
    """
    for _ in range(10):
        msg = consumer.poll(timeout=5)
        if msg is None:
            yield {"message": "No message received"}
        elif msg.error():
            return {"error": str(msg.error())}
        else:
            yield {"message": msg.value()}


@app.get("/check/broker")
def check_broker():
    """
    Simple endpoint to check if the broker is reachable.
    """
    try:
        metadata = consumer.list_topics(timeout=1)
        return {"status": "Broker is reachable",
                "topics": list(metadata.topics.keys())}
    except Exception as e:
        return {"status": "Broker is not reachable",
                "error": str(e)}