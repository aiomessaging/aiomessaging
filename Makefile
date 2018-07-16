ifeq ($(shell uname), Darwin)
	PTW_SUFFIX=--onfail "growlnotify -m \"Tests failed\""
else
	PTW_SUFFIX=
endif

export PYTHONPATH=.
PYTEST_COV_PARAMS=--cov-report=term-missing --cov aiomessaging --cov-report xml:cov.xml

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

run:
	python -m aiomessaging

send:
	python send.py
