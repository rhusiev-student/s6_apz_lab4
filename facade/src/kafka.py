from confluent_kafka import Producer
import json


def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def produce_message(message, topic_name="my-kool-topic"):
    """Send a message to the Kafka topic."""
    conf = {
        "bootstrap.servers": "localhost:19092,localhost:19094,localhost:19095",
        "acks": "all",
    }

    producer = Producer(conf)

    if not isinstance(message, str):
        message = json.dumps(message)

    producer.produce(
        topic_name, value=message.encode("utf-8"), callback=delivery_report
    )

    producer.flush()


if __name__ == "__main__":
    produce_message({"key": "my-kool-test-value", "timestamp": "1337-42-69"})
