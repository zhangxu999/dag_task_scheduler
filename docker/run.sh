#!/usr/bin/env bash

docker run -d -name task_scheduler --network="host" -v /home/xu/learningProject/data_processing/docker/graph.sqlite:/root/graph.sqlite dag_task_scheduler:0.2