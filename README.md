# Sales Data Pipeline

This project implements a complete data engineering pipeline for processing sales data, from ingestion to analytics-ready dimensional models based on the data exploration in [notebook](./misc/exploration.ipynb).

## Features

- **Data Ingestion**: Automated CSV processing with data quality checks
- **Data Storage**: PostgreSQL with proper schema design
- **Data Transformation**: Both Python-based transformation and dbt models
- **Orchestration**: Apache Airflow DAGs for end-to-end Python-based pipeline execution
- **Containerization**: Docker setup for reproducible environments

## Architecture

The pipeline follows a medallion architecture:

1. **Bronze Layer**: Raw data ingestion (raw.sales)
2. **Silver Layer**: Dimensional model with transformations (transformed.dim_\*, analytics.dim_\*)
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

Run the setup script to initialize Airflow and PostgreSQL:

```bash
chmod +x scripts/*.sh
./scripts/setup_airflow.sh
```

This will:
- Create necessary directories
- Build and start Docker containers for Airflow and PostgreSQL
- Set up an Airflow admin user
- Display access information for the Airflow UI

### 4. Access Airflow UI

Open http://localhost:8081 in your browser
- Username: `airflow`
- Password: `airflow`

### 5. Run the pipeline

Place your CSV files in the `data/` directory and:

1. Enable the `sales_data_pipeline` DAG in the Airflow UI
2. Trigger the DAG manually or let it run on its schedule

### 6. Run dbt models (optional)
In order to run dbt jobs, a local development environment must be installed.

```bash
# Set up dbt profile
mkdir -p ~/.dbt
cp misc/dbt_profile.yml ~/.dbt/profiles.yml
```

#### Runs models with various options for flexibility.

**Usage:**
```bash
./scripts/run_dbt.sh [OPTIONS]
```

**Options:**
- `-h, --help`: Show help message
- `-e, --env ENV`: Environment to run in (default: dev)
- `-m, --models NAME`: Specific models to run (comma-separated, no spaces)
- `-t, --test`: Run tests after models
- `-d, --docs`: Generate documentation
- `-f, --full`: Run full process (deps, run, test, docs)
- `-c, --clean`: Run 'dbt clean' before other commands
- `-s, --seed`: Run 'dbt seed' to load seed files

**Examples:**
```bash
# Run the full workflow (dependencies, models, tests, docs)
./scripts/run_dbt.sh -f

# Run only a specific model
./scripts/run_dbt.sh -m dim_location

# Run all models in the marts directory
./scripts/run_dbt.sh -m "marts.*"

# Run in production environment with tests
./scripts/run_dbt.sh -e prod -t
```

## Project Structure

```
.
├── airflow/
│   └── dags/                     # Airflow DAG definitions
├── data/                         # Input data directory
│   └── processed/                # Archived processed files
├── data_ingestion/               # ETL process
│   ├── ingest.py                 # Data ingestion script
│   ├── transform.py              # Data transformation script
│   └── utils.py                  # Shared utility functions
├── dbt_transform/                # dbt project
│   ├── models/
│   │   ├── marts/                # Dimensional models
│   │   └── staging/              # Staging models
│   └── macros/                   # dbt macros/tests
├── initdb/                       # Database initialization
├── misc/                         # Miscellaneous, including task description, data exploration and dbt profile
├── scripts/                      # Execution scripts
│   ├── setup-airflow.sh          # Airflow setup script
│   └── run_dbt.sh                # dbt running script
├── docker-compose.yml            # Docker Compose configuration
├── Dockerfile.airflow            # Airflow container definition
└── requirements-airflow          # Airflow dependencies
```

## Data Models

### Staging Models

- `stg_sales`: Cleaned and validated sales data

### Dimensional Models

- `dim_product`: Product information
- `dim_location`: Geographic locations
- `fact_sales`: Sales transactions with foreign keys

## Maintenance

### Logs

- Airflow logs: Available in the Airflow UI
- Python logs: Check `data_processing.log`

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

1. Edit `airflow/dags/sales_data_pipeline.py` to add or modify tasks
2. Modify `dbt_transform/dbt_project.yml` to configure dbt behavior

## Troubleshooting

- **Port conflicts**: If port 5433 is already in use, modify the port mapping in `docker-compose.yml`
- **Database connection issues**: Verify environment variables in `docker-compose.yml`
- **Missing modules**: Check installed packages in the containers vs `requirements-airflow.txt`
- **dbt errors**: Check `dbt_transform/target/` for detailed logs
