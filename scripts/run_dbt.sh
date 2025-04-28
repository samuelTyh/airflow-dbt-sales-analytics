#!/bin/bash
set -e

# Display help message
show_help() {
    echo "Usage: ./scripts/run_dbt.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help         Show this help message"
    echo "  -e, --env ENV      Environment to run in (dev, airflow, prod) (default: dev)"
    echo "  -m, --models NAME  Specific models to run (comma-separated, no spaces)"
    echo "  -t, --test         Run tests after models"
    echo "  -d, --docs         Generate documentation"
    echo "  -f, --full         Run full process (deps, run, test, docs)"
    echo "  -c, --clean        Run 'dbt clean' before other commands"
    echo "  -s, --seed         Run 'dbt seed' to load seed files"
    echo "  -a, --airflow      Run in Airflow container instead of locally"
    echo ""
    echo "Examples:"
    echo "  ./scripts/run_dbt.sh -f                     # Run full process locally (dev target)"
    echo "  ./scripts/run_dbt.sh -m dim_location        # Run only dim_location model locally"
    echo "  ./scripts/run_dbt.sh -m 'marts.*' -e prod   # Run all models in marts directory in prod env"
    echo "  ./scripts/run_dbt.sh -a -f                  # Run full process in Airflow container"
    echo "  ./scripts/run_dbt.sh -a -m stg_sales        # Run stg_sales model in Airflow container"
}

# Default values
ENV="dev"
MODELS="all"
RUN_TESTS=false
GENERATE_DOCS=false
RUN_DEPS=false
RUN_CLEAN=false
RUN_SEED=false
IN_AIRFLOW=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        -e|--env)
            ENV="$2"
            shift 2
            ;;
        -m|--models)
            MODELS="$2"
            shift 2
            ;;
        -t|--test)
            RUN_TESTS=true
            shift
            ;;
        -d|--docs)
            GENERATE_DOCS=true
            shift
            ;;
        -f|--full)
            RUN_DEPS=true
            RUN_TESTS=true
            GENERATE_DOCS=true
            shift
            ;;
        -c|--clean)
            RUN_CLEAN=true
            shift
            ;;
        -s|--seed)
            RUN_SEED=true
            shift
            ;;
        -a|--airflow)
            IN_AIRFLOW=true
            ENV="airflow"  # Force environment to airflow when running in container
            shift
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Navigate to the project root directory (one level up from scripts/)
PROJECT_ROOT="$(dirname "$(dirname "$0")")"

# Determine how to run dbt based on environment
if $IN_AIRFLOW; then
    echo "🚀 Running dbt in Airflow container with target: $ENV"
    
    # Set paths for running in Airflow container
    DBT_PROJECT_DIR="/opt/airflow/project/dbt_transform"
    DBT_PROFILES_DIR="/opt/airflow/dbt_profiles"
    
    # Define function to run commands in the Airflow container
    run_in_airflow() {
        docker-compose exec airflow-webserver bash -c "$1"
    }
    
    # Run dbt clean if requested
    if $RUN_CLEAN; then
        echo "🧹 Cleaning dbt project in Airflow container..."
        run_in_airflow "cd $DBT_PROJECT_DIR && dbt clean --profiles-dir $DBT_PROFILES_DIR --target $ENV"
    fi
    
    # Install dependencies if needed
    if $RUN_DEPS; then
        echo "📦 Installing dependencies in Airflow container..."
        run_in_airflow "cd $DBT_PROJECT_DIR && dbt deps --profiles-dir $DBT_PROFILES_DIR --target $ENV"
    fi
    
    # Run seed if requested
    if $RUN_SEED; then
        echo "🌱 Loading seed data in Airflow container..."
        run_in_airflow "cd $DBT_PROJECT_DIR && dbt seed --profiles-dir $DBT_PROFILES_DIR --target $ENV"
    fi
    
    # Run the models
    echo "🏗️  Building models in Airflow container: $MODELS..."
    if [ "$MODELS" = "all" ]; then
        run_in_airflow "cd $DBT_PROJECT_DIR && dbt run --profiles-dir $DBT_PROFILES_DIR --target $ENV"
    else
        run_in_airflow "cd $DBT_PROJECT_DIR && dbt run --models $MODELS --profiles-dir $DBT_PROFILES_DIR --target $ENV"
    fi
    
    # Run tests if requested
    if $RUN_TESTS; then
        echo "🧪 Running tests in Airflow container..."
        if [ "$MODELS" = "all" ]; then
            run_in_airflow "cd $DBT_PROJECT_DIR && dbt test --profiles-dir $DBT_PROFILES_DIR --target $ENV"
        else
            run_in_airflow "cd $DBT_PROJECT_DIR && dbt test --models $MODELS --profiles-dir $DBT_PROFILES_DIR --target $ENV"
        fi
    fi
    
    # Generate documentation if requested
    if $GENERATE_DOCS; then
        echo "📚 Generating documentation in Airflow container..."
        run_in_airflow "cd $DBT_PROJECT_DIR && dbt docs generate --profiles-dir $DBT_PROFILES_DIR --target $ENV"
        echo "✅ Documentation generated. To view, run:"
        echo "docker-compose exec airflow-webserver bash -c \"cd $DBT_PROJECT_DIR && dbt docs serve --profiles-dir $DBT_PROFILES_DIR --target $ENV\""
    fi
    
else
    # Local execution
    echo "🚀 Running dbt locally with target: $ENV"
    
    # Navigate to the dbt project directory
    cd "$PROJECT_ROOT/dbt_transform" || {
        echo "Error: Could not find the dbt_transform directory"
        exit 1
    }
    
    # Run dbt clean if requested
    if $RUN_CLEAN; then
        echo "🧹 Cleaning dbt project..."
        dbt clean --target "$ENV"
    fi
    
    # Install dependencies if needed
    if $RUN_DEPS; then
        echo "📦 Installing dependencies..."
        dbt deps --target "$ENV"
    fi
    
    # Run seed if requested
    if $RUN_SEED; then
        echo "🌱 Loading seed data..."
        dbt seed --target "$ENV"
    fi
    
    # Run the models
    echo "🏗️  Building models: $MODELS..."
    if [ "$MODELS" = "all" ]; then
        dbt run --target "$ENV"
    else
        dbt run --models "$MODELS" --target "$ENV"
    fi
    
    # Run tests if requested
    if $RUN_TESTS; then
        echo "🧪 Running tests..."
        if [ "$MODELS" = "all" ]; then
            dbt test --target "$ENV"
        else
            dbt test --models "$MODELS" --target "$ENV"
        fi
    fi
    
    # Generate documentation if requested
    if $GENERATE_DOCS; then
        echo "📚 Generating documentation..."
        dbt docs generate --target "$ENV"
        echo "✅ Documentation generated. Run 'dbt docs serve --target $ENV' to view it in your browser."
    fi
fi

echo "----------------------------------------"
echo "✨ dbt workflow completed successfully! ✨"