WORKER_COUNT ?= 8

check:
	poetry run ruff check --fix mapreduce/

format: 
	poetry run ruff format mapreduce/

run:
	poetry run python mapreduce/main.py

coordinator:
	poetry run python mapreduce/coordinator/coordinator.py $(ARGS)

worker:
	rm -rf output
	for i in $$(seq 1 $(WORKER_COUNT)); do \
		poetry run python mapreduce/worker/worker.py $(ARGS) & \
	done; \
	wait; \
	rm -rf intermediate
	cat output/mr-out-* | sort > mr-out


.PHONY: check format run coordinator
.PHONY: check format run worker 