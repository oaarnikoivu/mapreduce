import argparse
import threading
from typing import Any
from xmlrpc.server import SimpleXMLRPCServer

from models.status import Status
from models.task import Task
from models.type import Type


class Coordinator:
    def __init__(self, input_files: list[str], n_reduce: int):
        self.input_files = input_files
        self.task_queue = [
            Task(id=i, type=Type.MAP, name=input_files[i], status=Status.READY)
            for i in range(len(input_files))
        ]
        self.lock = threading.Lock()

    def get_task(self) -> dict[str, Any]:
        with self.lock:
            task = next(task for task in self.task_queue if task.status == Status.READY)
            return task.model_dump()

    def update_task(self, id: int, type: str, status: str) -> None:
        with self.lock:
            task = next(iter(task for task in self.task_queue if task.id == id), None)
            if task:
                task.type = type
                task.status = status

    def has_task(self) -> bool:
        return any(task.status == Status.READY for task in self.task_queue)


def run_coordinator() -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--port", type=int, required=True, help="MapReduce coordinator port."
    )
    parser.add_argument(
        "--input", type=str, required=True, help="MapReduce input files."
    )
    args = parser.parse_args()

    port = args.port
    input_files = args.input.split(",")

    server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
    rpc_server = Coordinator(input_files=input_files, n_reduce=10)
    server.register_instance(rpc_server)

    print(f"RPC server listening on port {port}...")
    server.serve_forever()


if __name__ == "__main__":
    run_coordinator()
