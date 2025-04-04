import logging
from fastapi import FastAPI, HTTPException
import yaml
from pydantic import BaseModel, field_validator
from ipaddress import ip_address
import asyncio
from contextlib import asynccontextmanager
import argparse

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

app = FastAPI()


class IPSet(BaseModel):
    ip: str
    port: int

    @field_validator("ip")
    @classmethod
    def validate_ip(cls, v):
        try:
            ip_address(v)
            return v
        except ValueError:
            raise ValueError(f"Invalid IP address format: {v}")

    @field_validator("port")
    @classmethod
    def validate_port(cls, v):
        if not (1 <= v <= 65535):
            raise ValueError(f"Port number must be between 1 and 65535: {v}")
        return v


file_lock = asyncio.Lock()


async def load_ip_mappings() -> dict[str, dict[str, str]]:
    try:

        def _load_yaml():
            with open("config.yaml", "r") as file:
                return yaml.safe_load(file)

        data = await asyncio.to_thread(_load_yaml)
        ip_mappings = data.get("ip_mappings", {}) if data else {}
        logger.info("Successfully loaded IP mappings from YAML.")
        return ip_mappings

    except Exception as e:
        logger.error(f"Error loading YAML file: {e}")
        return {}


async def save_ip_mappings(ip_mappings: dict[str, dict[str, str]]) -> bool:
    try:

        def _save_yaml():
            with open("config.yaml", "w") as file:
                yaml.safe_dump({"ip_mappings": ip_mappings}, file)
            return True

        result = await asyncio.to_thread(_save_yaml)
        logger.info("Successfully saved IP mappings to YAML.")
        return result

    except Exception as e:
        logger.error(f"Error saving YAML file: {e}")
        return False


ip_mappings: dict[str, dict[str, str]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ip_mappings
    ip_mappings = await load_ip_mappings()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/get_ip/{service}/{num}")
async def get_ip(service: str, num: str):
    if service not in ip_mappings:
        logger.warning(f"Service '{service}' not found in IP mappings.")
        raise HTTPException(
            status_code=404, detail=f"No IP:Port found for service {service}"
        )

    if num not in ip_mappings[service]:
        logger.warning(f"Number '{num}' not found for service '{service}'.")
        raise HTTPException(
            status_code=404,
            detail=f"No IP:Port found for number {num}, service {service}",
        )

    logger.info(
        f"Retrieved IP mapping for {service}:{num} -> {ip_mappings[service][num]}"
    )
    return {"num": num, "ip_port": ip_mappings[service][num]}


@app.post("/set_ip/{service}/{num}")
async def set_ip(service: str, num: str, ip_data: IPSet):
    global ip_mappings

    ip_port = f"{ip_data.ip}:{ip_data.port}"

    async with file_lock:
        if service not in ip_mappings:
            logger.warning(f"Service '{service}' not found when setting IP.")
            raise HTTPException(
                detail="Failed to find service in IP:Port mapping", status_code=404
            )

        ip_mappings[service][num] = ip_port
        if not await save_ip_mappings(ip_mappings):
            logger.error(f"Failed to save IP:Port mapping for {service}:{num}")
            raise HTTPException(
                status_code=500, detail="Failed to save IP:Port mapping"
            )

        logger.info(f"Set IP mapping: {service}:{num} -> {ip_port}")
        return {"success": True, "num": num, "ip_port": ip_port}


@app.get("/get_ips/{service}")
async def get_ips(service: str):
    if service not in ip_mappings:
        logger.warning(f"Service '{service}' not found in IP mappings.")
        raise HTTPException(
            status_code=404, detail=f"No IP:Port found for service {service}"
        )

    logger.info(f"Retrieved all IP mappings for service '{service}'.")
    return ip_mappings[service]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-config", action="store_true")
    args = parser.parse_args()
    new_config: bool = args.new_config

    if new_config:
        try:
            with open("config.yaml", "w") as file:
                yaml.safe_dump(
                    {
                        "ip_mappings": {
                            "logging": {},
                            "messages": {},
                        }
                    },
                    file,
                )
            logger.info("Created new config.yaml file with default structure.")
        except Exception as e:
            logger.error(f"Failed to create new config.yaml: {e}")

    import uvicorn

    logger.info("Starting FastAPI server on 0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
