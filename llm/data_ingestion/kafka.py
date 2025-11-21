# -*- coding: UTF-8 -*-
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, admin
import json
import sys

BOOTSTRAP_SERVERS = 'localhost:9092'


def create_topic(topic='test'):
    """Create a Kafka topic using Confluent Admin API."""
    admin_client = admin.AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    topic_list = [admin.NewTopic(topic, num_partitions=1, replication_factor=1)]
    existing = admin_client.list_topics(timeout=5).topics
    if topic in existing:
        print(f"Topic '{topic}' already exists.")
        return
    fs = admin_client.create_topics(topic_list)
    for topic_name, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic_name} created.")
        except Exception as e:
            print(f"Failed to create topic {topic_name}: {e}")


def delete_topic(topic='test'):
    """Delete Kafka topic."""
    admin_client = admin.AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    existing = admin_client.list_topics(timeout=5).topics
    if topic in existing:
        fs = admin_client.delete_topics([topic])
        for t, f in fs.items():
            try:
                f.result()
                print(f"Topic {t} deleted.")
            except Exception as e:
                print(f"Failed to delete topic {t}: {e}")
    else:
        print(f"Topic {topic} does not exist.")


def send_msg(topic='test', msg=None):
    """Send a JSON message to Kafka."""
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    if msg is not None:
        producer.produce(topic, json.dumps(msg).encode('utf-8'), callback=delivery_report)
        producer.flush()


def get_msg(topic='test'):
    """Consume messages from Kafka."""
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    print(f"Subscribed to topic '{topic}'")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Received: {data}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()


if __name__ == '__main__':
    topic = 'user'
    delete_topic(topic)
    create_topic(topic)

    msgs = [
        {'a': 'a', 'b': 1, 'c': 1, 'time': '2013-01-01T00:14:13Z'},
        {'a': 'b', 'b': 2, 'c': 2, 'time': '2013-01-01T00:24:13Z'},
        {'a': 'a', 'b': 3, 'c': 3, 'time': '2013-01-01T00:34:13Z'}
    ]

    for msg in msgs:
        send_msg(topic, msg)

    get_msg(topic)
