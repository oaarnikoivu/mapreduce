import argparse
import errno
import glob
import hashlib
import importlib
import json
import os
import uuid
import xmlrpc.client
from collections import defaultdict
from pathlib import Path
from typing import Callable

from loguru import logger

from mapreduce.models.key_value import KeyValue
from mapreduce.models.task import MapTask, ReduceTask

MapFunc = Callable[[str, str], list[KeyValue]]
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
        n_reduce = self.server.get_num_reduce()

        with open(f"{DATA_DIR}/{task.name}") as f:
            content = f.read()

        kva = self.mapf(task.name, content)
        kva.sort(key=lambda x: x.key)

        intermediate = defaultdict(list)
        for kv in kva:
            partition = self.ihash(kv.key) % n_reduce
            intermediate[partition].append(kv)

        for partition, kva in intermediate.items():
            intermediate_file = f"mr-{task.id}-{partition}"
            partitioned_kva = [item.model_dump() for item in kva]
            with open(f"{INTERMEDIARY_DIR}/{intermediate_file}.json", "w") as of:
                json.dump(partitioned_kva, of)

        self.server.complete_map_task(task.id)

    def reducer(self, task: ReduceTask):
        partition = task.partition

        intermediate: list[KeyValue] = []

        file_paths = glob.glob(os.path.join(INTERMEDIARY_DIR, f"*-{partition}.json"))
        for file_path in file_paths:
            if os.path.isfile(file_path):
                with open(file_path, encoding="utf-8") as file:
                    kva = json.load(file)
                    kva = [KeyValue.model_validate(item) for item in kva]
                    intermediate.extend(kva)

        intermediate.sort(key=lambda x: x.key)

        tmp = defaultdict(list)
        for item in intermediate:
            tmp[item.key].append(item.value)

        out_file = f"mr-out-{partition}"
        with open(f"{OUT_DIR}/{out_file}", "w") as of:
            for key, values in tmp.items():
                val = self.reducef(key, values)
                of.write(f"{key} {val}")
                of.write("\n")

        self.server.complete_reduce_task(partition)

    @staticmethod
    def ihash(key):
        h = hashlib.sha1(key.encode())
        return int(h.hexdigest(), 16) & 0x7FFFFFFF

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

    worker = Worker(task=task, coordinator_port=cport)
    worker()


if __name__ == "__main__":
    run_worker()
