import argparse
import errno
import importlib
import json
import os
import uuid
import xmlrpc.client
from pathlib import Path
from typing import Callable

from loguru import logger

from mapreduce.models.task import MapTask, ReduceTask

MapFunc = Callable[[str, str], list[dict[str, str]]]
ReduceFunc = Callable[[str, list[str]], str]

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = os.path.join(ROOT_DIR, "data")
INTERMEDIARY_DIR = os.path.join(ROOT_DIR, "intermediate")
OUT_DIR = os.path.join(ROOT_DIR, "output")

try:
    os.makedirs(INTERMEDIARY_DIR)
except OSError as err:
    if err.errno != errno.EEXIST:
        raise

try:
    os.makedirs(OUT_DIR)
except OSError as err:
    if err.errno != errno.EEXIST:
        raise


class Worker:
    def __init__(self, task: str, coordinator_port: int) -> None:
        self.worker_id = uuid.uuid4()
        self.server = xmlrpc.client.ServerProxy(f"http://localhost:{coordinator_port}")
        self.mapf, self.reducef = self.load_plugin(task=task)

    def __call__(self):
        while not self.server.map_done():
            task_info = self.server.get_map_task()
            if task_info:
                task = MapTask.model_validate_json(task_info)
                self.mapper(task=task)

        while not self.server.reduce_done():
            task_info = self.server.get_reduce_task()
            if task_info:
                task = ReduceTask.model_validate_json(task_info)
                self.reducer(task=task)

    def mapper(self, task: MapTask) -> None:
        with open(f"{DATA_DIR}/{task.name}") as f:
            content = f.read()
            kva = self.mapf(task.name, content)

        partitions = {}
        for kv in kva:
            for key, value in kv.items():
                partition = self.partitioner(key=key, n_reduce=10)
                partitions.setdefault(partition, []).append({key: value})

        for partition, data in partitions.items():
            out_file = f"mr-{task.id}-{partition}"
            with open(f"{INTERMEDIARY_DIR}/{out_file}.json", "a") as of:
                json.dump(data, of)

        self.server.complete_map_task(task.id)

    def reducer(self, task: ReduceTask):
        partition = task.partition

        intermediate: list[list[dict[str, str]]] = []
        for dir_path, _, file_names in os.walk(INTERMEDIARY_DIR):
            for file_name in file_names:
                if file_name.endswith(f"-{partition}.json"):
                    file_path = os.path.join(dir_path, file_name)
                    if os.path.isfile(file_path):
                        with open(file_path, encoding="utf-8") as file:
                            kva = json.load(file)
                            intermediate.append(kva)

        shuffled_data = {}
        for items in intermediate:
            for item in items:
                for key, value in item.items():
                    if key not in shuffled_data:
                        shuffled_data[key] = [value]
                    else:
                        shuffled_data[key].append(value)

        for key, values in shuffled_data.items():
            val = self.reducef(key, values)

            out_file = f"mr-out-{partition}"
            with open(f"{OUT_DIR}/{out_file}", "a") as of:
                of.write(f"{key} {val}")
                of.write("\n")

        self.server.complete_reduce_task(task.partition)

    @staticmethod
    def partitioner(key: str, n_reduce: int) -> int:
        hash_code = hash(key)
        partition = hash_code % n_reduce
        return partition

    @staticmethod
    def load_plugin(task: str) -> tuple[MapFunc, ReduceFunc]:
        module = importlib.import_module(f"mapreduce.tasks.{task}")

        try:
            mapf = module.map
        except Exception as err:
            logger.error(err)

        try:
            reducef = module.reduce
        except Exception as err:
            logger.error(err)

        return mapf, reducef


def run_worker() -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--task", type=str, required=True, help="MapReduce task.")
    parser.add_argument(
        "--cport", type=int, required=True, help="MapReduce Coordinator port."
    )
    args = parser.parse_args()

    task = args.task
    cport = args.cport

    Worker(task=task, coordinator_port=cport)()


if __name__ == "__main__":
    run_worker()
