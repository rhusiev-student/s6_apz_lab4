import asyncio
import sys
import logging
import requests
from kafka import KafkaConsumerError, consume_messages
from fastapi import FastAPI
import uvicorn
from threading import Thread
from collections import OrderedDict
from threading import Lock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("kafka_consumer.log")],
)

logger = logging.getLogger(__name__)


class SafeOrderedDict:
    def __init__(self, maxsize=1000):
        self.data = OrderedDict()
        self.lock = Lock()
        self.maxsize = maxsize

    def add(self, key, value):
        with self.lock:
            self.data[key] = value
            while len(self.data) > self.maxsize:
                self.data.popitem(last=False)

    def get_all(self):
        with self.lock:
            return list(self.data.items())


message_store = SafeOrderedDict()

app = FastAPI()


@app.get("/")
async def get_messages():
    return {"messages": message_store.get_all()}


async def kafka_consumer():
    consumer_gen = consume_messages(topic_name="my-kool-topic")
    try:
        message_counter = 0
        for message in consumer_gen:
            if message is None:
                continue
            if isinstance(message, KafkaConsumerError):
                if message.partition is None:
                    logger.critical(
                        "Breaking loop due to consumer error",
                    )
                    break
            else:
                logger.info(f"Received message: {message}")
                message_store.add(message_counter, message)
                message_counter += 1
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.exception(f"Unexpected error in main consumer loop: {e}")


def run_fastapi(port: int):
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    logger.info("Starting Kafka consumer application")
    args = sys.argv
    if len(args) != 4:
        logger.error(f"Usage: {args[0]} <config_url> <self_ip> <num>")
        sys.exit(1)

    config_url = args[1]
    self_ip = args[2]
    num = args[3]

    try:
        num = int(num)
    except ValueError:
        logger.error(f"Invalid number: {num}")
        sys.exit(1)

    logger.info(
        f"Starting Kafka consumer with config URL: {config_url}, self IP: {self_ip}"
    )

    port = 12227 + num
    data = {"port": str(port), "ip": self_ip}
    try:
        logger.debug(f"Notifying config service at {config_url}/set_ip/messages/0")
        response = requests.post(f"{config_url}/set_ip/messages/{num}", json=data)

        if not response.ok:
            logger.error(
                f"Error notifying config service: {response.status_code} - {response.text}"
            )
            exit(1)
        logger.info("Successfully notified config service")
    except Exception as e:
        logger.exception(f"Error notifying config service: {e}")
        exit(1)

    api_thread = Thread(target=run_fastapi, args=(port,), daemon=True)
    api_thread.start()

    while True:
        logger.info("Starting to consume messages from Kafka topic 'my-kool-topic'")
        asyncio.run(kafka_consumer())
        logger.warning("Main function completed, restarting...")
