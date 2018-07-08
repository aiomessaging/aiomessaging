ifeq ($(shell uname), Darwin)
	PTW_SUFFIX=--onfail "growlnotify -m \"Tests failed\""
else
	PTW_SUFFIX=
endif

export PYTHONPATH=.
PYTEST_PARAMS=--cov-report=term-missing --cov aiomessaging

clean: pyclean
	rabbitmqctl stop_app
	rabbitmqctl reset
	rabbitmqctl start_app

pyclean:
	find . -type d -name "__pycache__" -delete

test: pyclean
	pytest ${PYTEST_PARAMS}

test-s:
	pytest ${PYTEST_PARAMS} -s

test-x:
	pytest ${PYTEST_PARAMS} -x

test-watch:
	ptw -- ${PYTEST_PARAMS}

run:
	python -m aiomessaging.app

send:
	python send.py
