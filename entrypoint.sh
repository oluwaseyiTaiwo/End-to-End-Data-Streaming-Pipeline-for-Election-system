#!/bin/bash
# Initialize Airflow DB
export PATH="/root/.local/bin:$PATH"
airflow db migrate
# Create default admin user if not exists
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin || true
# Start the webserver
exec airflow "$@"