export PYTHONPATH=.
PYTEST_COV_PARAMS=--cov-report=term-missing --cov aiomessaging --cov-report xml:cov.xml --durations=5

.PHONY: docs

run:
	python -m aiomessaging worker -c example.yml

send:
	python -m aiomessaging send example_event --count 1 -c example.yml

docs:
	$(MAKE) -C docs/ html
	open docs/_build/html/index.html

clean: pyclean
	rabbitmqctl stop_app
	rabbitmqctl reset
	rabbitmqctl start_app

pyclean:
	find . -type d -name "__pycache__" -delete

test: pyclean
	pytest ${PYTEST_COV_PARAMS}

test-s:
	pytest -s

test-x:
	pytest -x

test-watch:
	ptw -- --testmon

lint:
	pylint aiomessaging
	mypy aiomessaging --ignore-missing-imports
