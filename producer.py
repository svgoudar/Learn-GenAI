from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

data = {"id": 1, "message": "hello"}
producer.send("my_topic", value=data)
producer.flush()
