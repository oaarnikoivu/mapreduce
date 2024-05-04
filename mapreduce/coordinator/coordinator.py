import argparse
import os
import threading
import time
from pathlib import Path
from typing import Any
from xmlrpc.server import SimpleXMLRPCServer

from loguru import logger

from mapreduce.models.status import Status
from mapreduce.models.task import MapTask, ReduceTask

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = os.path.join(ROOT_DIR, "data")


class Coordinator:
    def __init__(self, input_files: list[str], n_reduce: int):
        self.input_files = input_files
        self.n_reduce = n_reduce

        self.map_task_queue = [
            MapTask(id=i, name=input_files[i], status=Status.READY)
            for i in range(len(input_files))
        ]

        self.reduce_task_queue = [
            ReduceTask(partition=i, status=Status.READY) for i in range(n_reduce)
        ]

        self.lock = threading.Lock()

    def get_num_reduce(self) -> int:
        return self.n_reduce

    def get_map_task(self) -> dict[str, Any] | None:
        with self.lock:
            try:
                task = next(
                    task for task in self.map_task_queue if task.status == Status.READY
                )
            except StopIteration:
                return None

            task.status = Status.RUNNING
            return task.model_dump_json()

    def complete_map_task(self, id: int) -> None:
        with self.lock:
            try:
                task = next(task for task in self.map_task_queue if task.id == id)
            except StopIteration:
                raise

            task.status = Status.DONE

    def map_done(self) -> bool:
        return all(task.status == Status.DONE for task in self.map_task_queue)

    def get_reduce_task(self) -> dict[str, Any] | None:
        with self.lock:
            try:
                task = next(
                    task
                    for task in self.reduce_task_queue
                    if task.status == Status.READY
                )
            except StopIteration:
                return None

            task.status = Status.RUNNING
            return task.model_dump_json()

    def complete_reduce_task(self, partition: int) -> None:
        with self.lock:
            try:
                task = next(
                    task
                    for task in self.reduce_task_queue
                    if task.partition == partition
                )
            except StopIteration:
                raise

            task.status = Status.DONE

    def reduce_done(self) -> bool:
        return all(task.status == Status.DONE for task in self.reduce_task_queue)


def check_tasks_and_stop_server(coordinator: Coordinator, server: SimpleXMLRPCServer):
    while True:
        if coordinator.map_done() and coordinator.reduce_done():
            logger.info("All tasks completed. Stopping the server.")
            server.shutdown()
            break
        time.sleep(5)


def run_coordinator() -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--port", type=int, required=True, help="MapReduce coordinator port."
    )
    args = parser.parse_args()

    port = args.port

    input_files = os.listdir(DATA_DIR)
    server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
    rpc_server = Coordinator(input_files=input_files, n_reduce=10)
    server.register_instance(rpc_server)

    logger.info(f"Coordinator listening on port {port}...")

    stop_check_thread = threading.Thread(
        target=check_tasks_and_stop_server, args=(rpc_server, server)
    )
    stop_check_thread.start()

    server.serve_forever()


if __name__ == "__main__":
    run_coordinator()
