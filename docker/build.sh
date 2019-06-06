#!/usr/bin/env bash
rm -rf add/*
cp -r ../config ../elements_config  ../workers ../elements.py ../logging.json \
    ../manager.py ../start.py ../utils.py ../worker_base.py ../superset_stat.py add/

docker build --no-cache --rm --network="host" -t dag_task_scheduler:0.2 ./
