#!/bin/bash

docker run -d -p 5672:5672 -p 15672:15672 --name rabbit rabbitmq:management
docker run --rm -d --name redis -p 6379:6379 redis

if ! nc -z localhost 5432; then
    echo 'starting docker "postgres" on port 5432' >&2
    docker run --rm -d --name postgres -p 5432:5432 \
        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=later_test \
        postgres
    echo "Waiting for Postgres to be ready..."
    sleep 5

    echo "Running migration"
    export DATABASE_URL=postgres://test:test@localhost/later_test
    cd later/
    sqlx database drop -y
    sqlx database create
    sqlx migrate run
    cd ../
fi