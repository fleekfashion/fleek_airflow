#!/usr/bin/env bash

CMD="airflow"
TRY_LOOP="10"

if [ "$@" = "webserver" ] || [ "$@" = "worker" ] || [ "$@" = "scheduler" ] ; then
    i=0
    while ! nc -z $POSTGRES_HOST $POSTGRES_PORT >/dev/null 2>&1 < /dev/null; do
        i=`expr $i + 1`
        if [ $i -ge $TRY_LOOP ]; then
            echo "$(date) - ${POSTGRES_HOST}:${POSTGRES_PORT} still not reachable, giving up"
            exit 1
        fi
        echo "$(date) - waiting for ${POSTGRES_HOST}:${POSTGRES_PORT}... $i/$TRY_LOOP"
        sleep 5
    done

    echo "Initialize database..."
    $CMD db init 
    $CMD users create --role Admin --username kian --email ghodoussikian@gmail.com --firstname Kian --lastname Ghodoussi --password $KIAN_PASSWORD 
    ./database_setup.sh
    databricks configure <<!
$DATABRICKS_HOST
$DATABRICKS_USERNAME
$DATABRICKS_PASSWORD
$DATABRICKS_PASSWORD
!
    sleep 5
fi

exec $CMD "$@"
