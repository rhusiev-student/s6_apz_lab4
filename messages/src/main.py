import sys
import requests
from fastapi import FastAPI
import uvicorn


def main():
    args = sys.argv
    if len(args) != 3:
        print(f"Usage: {args[0]} <config_url> <self_ip>")
        return

    config_url = args[1]
    self_ip = args[2]

    data = {"port": "13227", "ip": self_ip}
    try:
        response = requests.post(f"{config_url}/set_ip/messages/0", json=data)

        if not response.ok:
            print(f"Error notifying config: {response}")
            return
    except Exception as e:
        print(f"Error notifying config: {e}")
        return

    app = FastAPI()

    @app.get("/")
    async def root():
        return "Not implemented"

    # Start the server
    uvicorn.run(app, host="0.0.0.0", port=13227)


if __name__ == "__main__":
    main()
