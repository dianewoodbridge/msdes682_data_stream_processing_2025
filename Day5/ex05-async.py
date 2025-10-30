from concurrent.futures import ThreadPoolExecutor
import os

import asyncio
from confluent_kafka import Consumer
from dotenv import load_dotenv
from fastapi import FastAPI
from starlette.responses import StreamingResponse


load_dotenv()
app = FastAPI()
executor = ThreadPoolExecutor()

conf = {
    'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.environ['CONFLUENT_API_KEY'],
    'sasl.password': os.environ['CONFLUENT_API_SECRET'],
    'group.id': 'async-consumer-group-2',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['system.usage'])

# Helper async generator to poll Kafka
async def async_kafka_poll():
    loop = asyncio.get_running_loop()

    while True:
        # Poll in a thread executor to avoid blocking the event loop
        msg = await loop.run_in_executor(executor,
                                         consumer.poll,
                                         1)
        if msg is None:
            yield (b'{"message": "No message received"}\n')
        elif msg.error():
            yield (f'{{"error": "{msg.error()}"}}\n'.encode())
            break
        else:
            # Consider decoding to UTF-8
            message = msg.value().decode('utf-8')
            yield (f'{{"message": "{message}"}}\n'.encode())

@app.get("/consume/async")
async def consume_async():
    """
    Async version. 
    Streams messages as they arrive.
    Each poll uses a thread to avoid blocking the event loop.
    """
    return StreamingResponse(async_kafka_poll(),
                             media_type="application/json")

@app.get("/check/broker")
async def check_broker():
    """
    Async endpoint to check if the broker is reachable.
    """
    loop = asyncio.get_running_loop()
    try:
        metadata = await loop.run_in_executor(executor, consumer.list_topics)
        return {"status": "Broker is reachable",
                "topics": list(metadata.topics.keys())}
    except Exception as e:
        return {"status": "Broker is not reachable",
                "error": str(e)}
