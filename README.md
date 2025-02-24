# Senior Data Engineer Take-Home Assignment


👋 Welcome to the Hostaway Data Engineer Technical Test

## Objective
This assignment will test your skills in building an efficient data pipeline that processes CSV data, loads it into a database, orchestrates with Airflow, and transforms data using dbt.

## Requirements

### Part 1: Data Ingestion & Processing
- Create a script to ingest and parse the provided CSV file `./generated-sales-data.csv`
- Handle common data quality issues (missing values, duplicates)
- Design for incremental loading capabilities (optional)

### Part 2: Database Integration
- Load the processed data into PostgreSQL with appropriate schema design (docker compose provided)
- Implement basic indexing for performance optimization

### Part 3: Airflow Orchestration
- Create an Airflow DAG to orchestrate the pipeline
- Set up task dependencies that reflect the data flow

### Part 4: dbt Implementation
- Set up a dbt project to transform the raw data
- Create at least one staging model and one dimensional model
- Include basic tests for your models

### Part 5: Documentation
- Provide a README explaining your approach and setup instructions

## Provided Resources
- `generated-sales-data.csv`: Contains sales data
- Postgres docker compose 
- Connection details for PostgreSQL database

## Submission Guidelines
- Submit all code in a GitHub repository
- Complete the assignment within 5 days
- Be prepared to discuss your solution in a follow-up interview


### ---
### Start postgres
This creates a `sales` database
```bash
docker-compose up -d
```
*Note: if you have something running locally on port 5432 which will conflict with the postgres docker container then you can change the local port mapping in `docker-compose.yml` like so:*
```yaml
    ports:
      - "6543:5432"
``` 

### Good luck!