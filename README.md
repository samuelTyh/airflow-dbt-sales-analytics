# Sales Data Pipeline

This project implements a complete data engineering pipeline for processing sales data, from ingestion to analytics-ready dimensional models based on the data exploration in [notebook](./misc/exploration.ipynb).

## Features

- **Data Ingestion**: Automated CSV processing with data quality checks
- **Data Storage**: PostgreSQL with proper schema design
- **Data Transformation**: Both Python-based transformation and dbt models
- **Orchestration**: Apache Airflow DAGs for end-to-end pipeline execution
- **Containerization**: Docker setup for reproducible environments
- **Integrated dbt Workflow**: dbt executes as part of Airflow orchestration
- **Self-healing Pipeline**: Automatically checks for data existence and performs ingestion when needed

## Architecture

The pipeline follows a medallion architecture:

1. **Bronze Layer**: Raw data ingestion (raw.sales)
2. **Silver Layer**: Dimensional model with transformations (transformed.dim_*, analytics.dim_*)
3. **Gold Layer**: Analytics models (analytics.fact_sales, transformed.fact_sales)

## Prerequisites

- Docker and Docker Compose
- Git
- Python 3.12 (for local development)
- uv package manager (recommended for local development)

## Quick Start

### 1. Clone this repository

```bash
git clone https://github.com/samuelTyh/hostaway-interview.git
cd hostaway-interview
```

### 2. Local Development Setup (Optional)

For local development outside of Docker, recommend using the `uv` package manager for faster and more reliable Python dependency management:

```bash
# Install uv if you don't have it already
pip install uv

# Install Python
uv python install

# Create a virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv sync
```

The project is configured with a `.python-version` file specifying Python 3.12, which uv will respect automatically.

### 3. Set up the environment

Run the setup script to initialize Airflow, PostgreSQL, and dbt:

```bash
chmod +x scripts/*.sh
./scripts/setup.sh
```

This will:
- Create necessary directories
- Set up dbt profiles for both local and container execution
- Build and start Docker containers for Airflow and PostgreSQL
- Set up an Airflow admin user
- Display access information for the Airflow UI

### 4. Access Airflow UI

Open http://localhost:8081 in your browser
- Username: `airflow`
- Password: `airflow`

### 5. Run the pipeline

Place your CSV files in the `data/` directory and:

1. Enable both the `sales_data_pipeline` and `dbt_transform_pipeline` DAGs in the Airflow UI
2. Trigger either DAG manually or let them run on their schedules
3. Both pipelines will check for data in the database and perform ingestion if needed

### 6. Run dbt models (optional)

You can run dbt models either locally or within the Airflow container using the provided script:

```bash
# Run dbt locally
./scripts/run_dbt.sh -f              # Run full process (deps, run, test, docs)
./scripts/run_dbt.sh -m dim_location # Run only a specific model

# Run dbt in Airflow container
./scripts/run_dbt.sh -a -f           # Run full process in container
./scripts/run_dbt.sh -a -m marts.*   # Run all models in marts directory in container
```

For all available options:
```bash
./scripts/run_dbt.sh --help
```

## Project Structure

```
.
├── airflow/
│   ├── dags/                          # Airflow DAG definitions
│   │   ├── sales_data_pipeline.py     # ETL pipeline DAG
│   │   └── dbt_transform_dag.py       # dbt transformation DAG
│   └── plugins/
│       └── utils/                     # Shared utility functions
│           └── data_check.py          # Data existence check and auto-ingestion
├── data/                              # Input data directory
│   └── processed/                     # Archived processed files
├── data_ingestion/                    # ETL process
│   ├── ingest.py                      # Data ingestion script
│   ├── transform.py                   # Data transformation script
│   └── utils.py                       # Shared utility functions
├── dbt_profiles/                      # dbt profiles for Airflow container
├── dbt_transform/                     # dbt project
│   ├── models/
│   │   ├── marts/                     # Dimensional models
│   │   └── staging/                   # Staging models
│   └── macros/                        # dbt macros/tests
├── initdb/                            # Database initialization
├── misc/                              # Miscellaneous files
├── scripts/                           # Execution scripts
│   ├── setup.sh                       # Setup script for Airflow with dbt
│   └── run_dbt.sh                     # Enhanced dbt running script
├── docker-compose.yml                 # Docker Compose configuration
├── Dockerfile.airflow                 # Airflow container definition
└── requirements-airflow.txt           # Airflow dependencies
```

## Smart Data Pipeline Features

This pipeline includes several advanced features:

### Self-healing Data Flow

Both the ETL and dbt pipelines check for data existence before proceeding:

1. Each DAG automatically checks for data in the `raw.sales` table
2. If no data is found, ingestion is automatically triggered from the CSV
3. Once data is available, the transformations proceed
4. This allows either pipeline to be triggered independently without failures

### Custom Database Connection

The pipeline uses a custom database connection (`sales_db`) for all database operations:

1. Connection is automatically created during Airflow initialization
2. All tasks use this connection for consistent database access
3. The connection is defined via environment variables in `docker-compose.yml`

### Reusable Data Checking

The shared `data_check.py` utility provides:
- A custom sensor for checking data existence
- Functions for auto-ingestion when needed
- Task generation helpers for use in any DAG

## Data Models

### Staging Models

- `stg_sales`: Cleaned and validated sales data

### Dimensional Models

- `dim_product`: Product information
- `dim_location`: Geographic locations
- `fact_sales`: Sales transactions with foreign keys

## dbt Integration

The project uses a combined dbt profiles configuration that supports both local development and execution within the Airflow container. Key features:

- **Multiple Targets**: `dev` (local), `airflow` (container)
- **Enhanced Script**: Run dbt commands in either environment with a single script
- **Airflow Integration**: Automatic execution of dbt as part of the data pipeline

### dbt Local Setup

The dbt profile is automatically configured in `~/.dbt/profiles.yml` during setup. For manual setup:

```bash
mkdir -p ~/.dbt
cp dbt_profiles/profiles.yml ~/.dbt/profiles.yml
```

## Maintenance

### Logs

- Airflow logs: Available in the Airflow UI
- Python logs: Check `data_processing.log`
- dbt logs: Available in the Airflow UI task logs

### Database

The PostgreSQL database is accessible at:
- Host: localhost
- Port: 5433
- Username: postgres
- Password: mysecretpassword
- Database: sales

```bash
# Connect to the database
psql -h localhost -p 5433 -U postgres -d sales
```

## Development and Extension

### Adding New Data Sources

1. Modify `ingest.py` to handle new data formats
2. Update schema definitions in `transform.py`
3. Create corresponding dbt models in `dbt_transform/models/`

### Customizing the Pipeline

1. Edit `airflow/dags/sales_data_pipeline.py` to add or modify ETL tasks
2. Edit `airflow/dags/dbt_transform_dag.py` to customize dbt execution
3. Modify `dbt_transform/dbt_project.yml` to configure dbt behavior

## Troubleshooting

- **Port conflicts**: If port 5433 is already in use, modify the port mapping in `docker-compose.yml`
- **Database connection issues**: Verify environment variables in `docker-compose.yml`
- **Missing modules**: Check installed packages in the containers vs `requirements-airflow.txt`
- **dbt errors**: Check Airflow task logs or run `./scripts/run_dbt.sh -a debug` to diagnose
- **Profile issues**: Ensure the dbt profile is correctly set up in both locations