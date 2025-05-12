#!/bin/bash

# Initialize Airflow if DB doesn't exist
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
  airflow users create \
    --username admin \
    --password admin \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email admin@example.com
fi

# Start services
airflow webserver & airflow scheduler
