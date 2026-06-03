import json
import time
import uuid
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer

BUCKET_NAME = "ais-maritime-raw-uzair"
TOPIC_NAME = "ais_raw_events"
BOOTSTRAP_SERVER = "localhost:9092"

s3 = boto3.client("s3")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="ais-s3-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

buffer = []
BATCH_SIZE = 500
FLUSH_INTERVAL_SECONDS = 30
last_flush_time = time.time()

def upload_to_s3(events):
    now = datetime.now(timezone.utc)

    key = (
        f"raw/ais/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"ais_events_{uuid.uuid4()}.json"
    )

    body = "\n".join(json.dumps(event) for event in events)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Uploaded {len(events)} events to s3://{BUCKET_NAME}/{key}")

print("Kafka consumer started...")
print(f"Reading from topic: {TOPIC_NAME}")

for message in consumer:
    buffer.append(message.value)

    current_time = time.time()

    if len(buffer) >= BATCH_SIZE or (current_time - last_flush_time) >= FLUSH_INTERVAL_SECONDS:
        upload_to_s3(buffer)
        buffer = []
        last_flush_time = current_time
