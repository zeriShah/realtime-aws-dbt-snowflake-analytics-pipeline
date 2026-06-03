import asyncio
import json
import os
import time
import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def main():

    async with websockets.connect(
        "wss://stream.aisstream.io/v0/stream"
    ) as websocket:

        subscribe_message = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": [
               [[-90, -180], [90, 180]]
            ]
        }

        await websocket.send(
            json.dumps(subscribe_message)
        )

        print("Connected to AISStream...")
        print("Sending data to Kafka...")

        async for message in websocket:

            data = json.loads(message)

            event = {
                "ingestion_time": int(time.time()),
                "raw_data": data
            }

            producer.send(
                KAFKA_TOPIC,
                value=event
            )

            producer.flush()

            print("Event sent to Kafka")

asyncio.run(main())
