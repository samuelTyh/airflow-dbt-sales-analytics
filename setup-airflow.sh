#!/bin/bash
set -e

mkdir -p airflow/dags airflow/logs airflow/plugins data/processed
chmod +x initdb/create-multiple-postgres-databases.sh

# Set environment variable for Airflow UID
export AIRFLOW_UID=$(id -u)

echo "Setup completed. Now start Airflow"
docker-compose down -v --rmi local
docker-compose up -d
sleep 10
docker-compose exec airflow-webserver airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow
echo ""
echo "Airflow is running now, access the Airflow web interface at: http://localhost:8081"
echo "Default credentials: airflow / airflow"