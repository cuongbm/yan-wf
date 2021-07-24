.DEFAULT_GOAL := lint

# choco install make

conda:
	CALL conda activate celery-test

install:
	python setup.py install

test: install
	pytest

lint:
	flake8 ./src

rabbitmq:
	CALL .\deployment\start_rabbitmq.bat

celery_worker1:
	celery -A tests.test_workflow.test_celery worker --loglevel=INFO