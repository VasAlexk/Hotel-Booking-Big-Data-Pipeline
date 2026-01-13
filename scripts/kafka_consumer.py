from kafka import KafkaConsumer
import json

TOPIC = "SDMD-1097464-final"
BROKER = "150.140.142.67:9094"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer-read", 
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Listening for messages on topic: {TOPIC} ...")

message_count = 0  

try:
    for message in consumer:
        event = message.value
        message_count += 1
        print(f"Received: {event}")
except KeyboardInterrupt:
    print("Consumer stopped manually.")
finally:
    consumer.close()
    print(f"Συνολικός αριθμός μηνυμάτων που διαβάστηκαν: {message_count}")
