#!/bin/bash

docker run -d -p 5672:5672 -p 15672:15672 --name rabbit rabbitmq:management
docker run --rm -d --name redis -p 6379:6379 redis

