check:
	poetry run ruff check --fix mapreduce/

format: 
	poetry run ruff format mapreduce/

run:
	poetry run python mapreduce/main.py