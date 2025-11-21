from kafka import KafkaConsumer
import json
from torch import nn
nn.Module
consumer = KafkaConsumer(
    "my_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

for msg in consumer:
    print(msg.value)
# genai_book/docker-compose.yml