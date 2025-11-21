from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

chunks = [f"This is text chunk number {i}" for i in range(1, 501)]  # demo data
batch_size = 50

for i in range(0, len(chunks), batch_size):
    batch = chunks[i:i+batch_size]
    payload = {"batch_id": i // batch_size, "data": batch}
    producer.send("embedding_jobs", payload)
    print(f"Sent batch {payload['batch_id']} with {len(batch)} chunks")
    time.sleep(1)  # simulate spacing

producer.flush()
print("All batches sent.")
