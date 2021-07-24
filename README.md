# YAN Workflow engine

A simple workflow engine allow define tasks, run with resume

# Development guide

## Using make

**Install make:**

```
choco install make
```

**Start rabbitmq:**

```
make rabbitmq
```

**Start celery worker (Run it in separated shell):**

```
make celery_worker1
```

**Run tests:**

```
make test
```

## Install local package

Need to install package locally before run test

```
python setup.py install
```

Verify if package is installed correctly:

```
python
>>> from workflow.workflows import RunStatus
```

## Start celery worker

```
celery -A tests.test_workflow.test_celery worker --loglevel=INFO
```
