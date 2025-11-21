from kafka import KafkaConsumer

# from kafka import KafkaConsumer
import json
from langchain_classic.embeddings import OpenAIEmbeddings
from langchain_classic.vectorstores import FAISS

embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

consumer = KafkaConsumer(
    "embedding_jobs",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="embedding-workers",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

faiss_db = None

for msg in consumer:
    batch_id = msg.value["batch_id"]
    data = msg.value["data"]
    print(f"Processing batch {batch_id} with {len(data)} items...")

    # Create / update FAISS
    if faiss_db is None:
        faiss_db = FAISS.from_texts(data, embeddings)
    else:
        faiss_db.add_texts(data)

    faiss_db.save_local("faiss_index")
    print(f"Batch {batch_id} embedded and saved.")
