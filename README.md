# mapreduce

MapReduce architecture in Python

## Running the Coordinator

```bash
make coordinator ARGS="--port 8000"
```

## Running Workers

You can modify the number of workers to run in the `Makefile`. Currently it defaults to 8.

```bash
make worker ARGS="--task word_count --cport 8000"
```

where `--task` is the MapReduce task you would like to run, and `--cport` is the coordinator port.
