import asyncio
import logging
from hazelcaster import Hazelcaster

import argparse

from grpc import aio
from grpc_generated import logging_pb2_grpc
from log import Logger

import requests

logging.basicConfig()
logging.getLogger("hazelcast").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def serve(hazelcaster: Hazelcaster, num: int, ip_address: str, ip_config: str):
    with hazelcaster:
        logger.info(f"Started hazelcast on port {hazelcaster.port}")
        srv = aio.server()
        logging_pb2_grpc.add_LoggingServiceServicer_to_server(
            Logger(hazelcaster, logger), srv
        )

        port = num + 13228
        listen_address = f"0.0.0.0:{port}"
        srv.add_insecure_port(listen_address)

        logger.info(f"Starting server on {listen_address}")
        response = requests.post(
            f"http://{ip_config}:8000/set_ip/logging/{num}",
            json={"ip": ip_address, "port": port},
        )
        if response.status_code != 200:
            logger.error(f"Failed to set ip in config: {response}")
        logger.info(f"Started server on {listen_address}")
        await srv.start()
        await srv.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("num", type=int)
    parser.add_argument("--ip_address", type=str, default="127.0.0.1")
    parser.add_argument("--ip_config", type=str, default="127.0.0.1")
    args = parser.parse_args()
    num: int = args.num
    ip_address: str = args.ip_address
    ip_config: str = args.ip_config

    logger.info("Starting hazelcast logging server")
    hazelcaster = Hazelcaster("logging", num)
    asyncio.run(serve(hazelcaster, num, ip_address, ip_config))
