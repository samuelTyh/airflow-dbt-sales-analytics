#!/bin/bash
set -e

PROJECT_ROOT="$(dirname "$(dirname "$0")")"
cd "$PROJECT_ROOT"

# Create necessary directories
mkdir -p airflow/dags airflow/logs airflow/plugins data/processed dbt_profiles

# Set up dbt profiles with combined configuration
echo "Setting up dbt profiles..."
cat > dbt_profiles/profiles.yml <<EOL
dbt_transform:
  target: airflow  # Default target for the container
  outputs:
    # Local development environment
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: postgres
      password: mysecretpassword
      dbname: sales
      schema: dbt
      threads: 4
      keepalives_idle: 0
    
    # Airflow container environment
    airflow:
      type: postgres
      host: postgres
      port: 5432  # Internal Docker port
      user: postgres
      password: mysecretpassword
      dbname: sales
      schema: dbt
      threads: 4
      keepalives_idle: 0
EOL

echo "Copying profiles to local dbt location (if it exists)..."
# Check if local dbt directory exists, create it if not
if [ ! -d ~/.dbt ]; then
  mkdir -p ~/.dbt
fi

# Copy the same profile to the local dbt directory for local runs
cp dbt_profiles/profiles.yml ~/.dbt/profiles.yml

# Make init scripts executable
chmod +x initdb/create-multiple-postgres-databases.sh
chmod +x scripts/*.sh

# Set environment variable for Airflow UID
export AIRFLOW_UID=$(id -u)

echo "Setup completed. Now building and starting containers..."
docker-compose down -v --rmi local
docker-compose up -d --build

echo "Waiting for Airflow to initialize (this may take a minute)..."
sleep 30

echo "Setting up Airflow admin user..."
docker-compose exec airflow-webserver airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow

echo ""
echo "✅ Setup completed successfully!"
echo "The combined dbt profiles configuration has been set up for both:"
echo "- Local development (target: dev)"
echo "- Airflow container (target: airflow)"
echo ""
echo "Airflow is running now, access the Airflow web interface at: http://localhost:8081"
echo "Default credentials: airflow / airflow"
echo ""
echo "Both Python-based ETL and dbt transformations have been integrated into Airflow DAGs:"
echo "- sales_data_pipeline: Handles data ingestion and initial Python transformations"
echo "- dbt_transform_pipeline: Runs dbt models for dimensional modeling"
echo ""
echo "To trigger the complete pipeline, activate both DAGs in the Airflow UI and trigger sales_data_pipeline"
echo "The dbt_transform_pipeline will automatically start after the sales_data_pipeline completes."