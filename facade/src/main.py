import asyncio
import sys
import uuid
import random

from grpc import aio
import httpx
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Response, status
from fastapi.responses import PlainTextResponse

from grpc_generated import logging_pb2, logging_pb2_grpc

from kafka import produce_message

MAX_RETRIES = 3
RETRY_DELAY_MS = 500


class Client:
    def __init__(self, config_url):
        self.config_url = config_url
        self._lock = asyncio.Lock()
        self.http = httpx.AsyncClient()

    async def get_possible_addresses(self, service):
        async with self._lock:
            try:
                resp = await self.http.get(f"{self.config_url}/get_ips/{service}")
                resp.raise_for_status()
                ips = resp.json()
            except Exception as e:
                print(f"Error while getting addresses: {e}")
                return []
        return list(ips.values())


async def create_grpc_client(logging_url):
    options = [
        ("grpc.enable_http_proxy", 0),
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.connect_timeout_ms", 5000),
        ("grpc.max_receive_message_length", -1),
        ("grpc.max_send_message_length", -1),
    ]
    channel = aio.insecure_channel(logging_url, options=options)
    return logging_pb2_grpc.LoggingServiceStub(channel)


def format_grpc_error(err):
    if hasattr(err, "code") and hasattr(err, "details"):
        return f"gRPC error ({err.code()}): {err.details()}"
    return str(err)


def retry_decorator(max_retries=MAX_RETRIES, delay_ms=RETRY_DELAY_MS, refresh_fn=None):
    def decorator(fn):
        async def wrapper(*args, **kwargs):
            attempts = 0
            while True:
                try:
                    return await fn(*args, **kwargs)
                except Exception as err:
                    print(f"Error on attempt {attempts + 1}: {format_grpc_error(err)}")
                    if attempts >= max_retries:
                        raise
                    if refresh_fn:
                        await refresh_fn(err)
                    attempts += 1
                    await asyncio.sleep(delay_ms / 1000)

        return wrapper

    return decorator


app = FastAPI()


@app.post("/")
async def add_log(message: str, client: Client = Depends(lambda: app.state.client)):
    log_uuid = str(uuid.uuid4())

    async def refresh_url(_):
        current = app.state.current_url_logging
        total = len(app.state.logging_urls)
        app.state.current_url_logging = (current + 1) % total

    @retry_decorator(refresh_fn=refresh_url)
    async def make_request():
        current = app.state.current_url_logging
        urls = app.state.logging_urls
        grpc_client = await create_grpc_client(urls[current])
        log = logging_pb2.Log(uuid=log_uuid, message=message)
        return await grpc_client.AddLog(log)

    app.state.logging_urls = await client.get_possible_addresses("logging")
    app.state.current_url_logging = random.randint(0, len(app.state.logging_urls) - 1)

    if not app.state.logging_urls:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No logging service available",
        )

    try:
        await make_request()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add log",
        )

    try:
        produce_message({"message": message})
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add message",
        )
    return Response(status_code=status.HTTP_200_OK)


@app.get("/")
async def get_logs(client: Client = Depends(lambda: app.state.client)):
    app.state.messages_urls = await client.get_possible_addresses("messages")
    if not app.state.messages_urls:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No message service available",
        )
    app.state.current_url_messages = random.randint(0, len(app.state.messages_urls) - 1)

    async def refresh_url(_):
        current = app.state.current_url_messages
        total = len(app.state.messages_urls)
        app.state.current_url_messages = (current + 1) % total

    @retry_decorator(refresh_fn=refresh_url)
    async def make_request():
        current = app.state.current_url_messages
        urls = app.state.messages_urls
        async with httpx.AsyncClient() as ac:
            resp = await ac.get(f"http://{urls[current]}")
            resp.raise_for_status()
            return resp.text

    try:
        message_text = await make_request()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    async def refresh_url(_):
        current = app.state.current_url_logging
        total = len(app.state.logging_urls)
        app.state.current_url_logging = (current + 1) % total

    @retry_decorator(refresh_fn=refresh_url)
    async def make_request():
        current = app.state.current_url_logging
        urls = app.state.logging_urls
        grpc_client = await create_grpc_client(urls[current])
        req = logging_pb2.GetLogsRequest()
        return await grpc_client.GetLogs(req)

    app.state.logging_urls = await client.get_possible_addresses("logging")

    if not app.state.logging_urls:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No logging service available",
        )
    app.state.current_url_logging = random.randint(0, len(app.state.logging_urls) - 1)

    try:
        response = await make_request()
        return PlainTextResponse(f"{message_text}\n{response.logs_string}")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


def main():
    config_url = sys.argv[1] if len(sys.argv) == 2 else "http://localhost:8000"
    client = Client(config_url)
    app.state.client = client
    uvicorn.run(app, host="0.0.0.0", port=13226)


if __name__ == "__main__":
    main()
