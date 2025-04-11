from confluent_kafka import Consumer, KafkaError
import json
import logging
from typing import Any
from collections.abc import Generator


logger = logging.getLogger(__name__)


class KafkaConsumerError(Exception):
    """Kafka consumer error wrapper."""

    def __init__(self, error, topic=None, partition=None):
        self.error = error
        self.topic = topic
        self.partition = partition
        message = f"Kafka error: {error}"
        if topic:
            message += f" (Topic: {topic}"
            if partition is not None:
                message += f", Partition: {partition}"
            message += ")"
        super().__init__(message)


def consume_messages(
    topic_name="my-kool-topic", group_id="my-kool-consumer-group", timeout=1.0
) -> Generator[dict[str, Any] | KafkaConsumerError | None, None, None]:
    """
    Generator function that yields messages from the Kafka topic or errors.

    Args:
        topic_name: Kafka topic to consume
        group_id: Consumer group ID
        timeout: Polling timeout in seconds

    Yields:
        Dict containing message data or KafkaConsumerError
    """
    conf = {
        "bootstrap.servers": "localhost:19092,localhost:19094,localhost:19095",
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }

    logger.info(f"Setting up Kafka consumer for topic: {topic_name}, group: {group_id}")
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])
    logger.debug(f"Consumer subscribed to topic: {topic_name}")

    try:
        while True:
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition for topic: {topic_name}")
                    yield None
                else:
                    error = KafkaConsumerError(
                        msg.error(), msg.topic(), msg.partition()
                    )
                    logger.error(f"Error with msg: {error}")
                    yield error
            else:
                message_value = msg.value().decode("utf-8")
                try:
                    data = json.loads(message_value)
                    consumer.commit(msg)
                    logger.debug(
                        f"Message processed from topic: {msg.topic()}, partition: {msg.partition()}"
                    )
                    yield data
                except json.JSONDecodeError:
                    logger.warning(
                        f"Failed to parse JSON from message, returning raw value from {msg.topic()}"
                    )
                    consumer.commit(msg)
                    yield message_value

    except Exception as e:
        logger.exception(f"Unexpected error in Kafka consumer: {e}")
        yield KafkaConsumerError(str(e))
    finally:
        logger.info("Closing Kafka consumer")
        consumer.close()
