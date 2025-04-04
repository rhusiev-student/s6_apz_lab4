import subprocess
import hazelcast
import os
from pathlib import Path


class Hazelcaster:
    def __init__(self, cluster_name: str, num: int, logs_dir: str = "logs"):
        self.cluster_name = cluster_name
        self.map = None
        self.num = num
        self.hazelcast_process = None
        self.client = None
        self.logs_dir = Path(logs_dir)

        os.makedirs(self.logs_dir, exist_ok=True)

    @property
    def port(self) -> int:
        return self.num + 5701

    @property
    def log_file_path(self) -> Path:
        return self.logs_dir / f"hazelcast_{self.num}.log"

    def __enter__(self):
        log_file = open(self.log_file_path, "w")

        self.hazelcast_process = subprocess.Popen(
            [
                "podman",
                "run",
                "--rm",
                f"--name=hazelcast-{self.num}",
                "--network=hazelcast-network",
                "-e",
                f"HZ_NETWORK_PUBLICADDRESS=10.8.0.2:{self.port}",
                "-e",
                f"HZ_CLUSTERNAME={self.cluster_name}",
                "-p",
                f"{self.port}:5701",
                "hazelcast/hazelcast:5.3.8",
            ],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

        self.client = hazelcast.HazelcastClient(
            cluster_name=self.cluster_name,
        )
        self.map = self.client.get_map("logs").blocking()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.client is not None:
            self.client.shutdown()

        if self.hazelcast_process is not None:
            self.hazelcast_process.kill()

            if hasattr(self.hazelcast_process.stdout, "close"):
                self.hazelcast_process.stdout.close()
