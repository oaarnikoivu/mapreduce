check:
	poetry run ruff check --fix mapreduce/

format: 
	poetry run ruff format mapreduce/

run:
	poetry run python mapreduce/main.py

coordinator:
	poetry run python mapreduce/coordinator/coordinator.py $(ARGS)

worker:
	poetry run python mapreduce/worker/worker.py $(ARGS)

.PHONY: check format run coordinator
.PHONY: check format run worker 