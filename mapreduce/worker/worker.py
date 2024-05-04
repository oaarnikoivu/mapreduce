import argparse
import importlib
import json
import os
import xmlrpc.client
from pathlib import Path
from typing import Callable

from loguru import logger

from mapreduce.models.task import Task
from mapreduce.models.type import Type

MapFunc = Callable[[str, str], list[dict[str, str]]]
ReduceFunc = Callable[[str, list[str]], str]

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = os.path.join(ROOT_DIR, "data")


class Worker:
    def __init__(self, task: str, coordinator_port: int) -> None:
        self.server = xmlrpc.client.ServerProxy(f"http://localhost:{coordinator_port}")
        self.mapf, self.reducef = self.load_plugin(task=task)

    def __call__(self):
        while self.server.has_task():
            task = Task(**self.server.get_task())

            id = task.id
            name = task.name

            if task.type == Type.MAP:
                with open(f"{DATA_DIR}/{name}") as f:
                    content = f.read()
                    kva = self.mapf(name, content)
                    out_file = f"mr-{id}"
                    with open(out_file, "w") as of:
                        json.dump(kva, of)
                self.server.update_task(id, "reduce", "ready")
            elif task.type == Type.REDUCE:
                self.server.update_task(id, "reduce", "done")
            else:
                raise NotImplementedError

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
