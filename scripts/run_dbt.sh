#!/bin/bash
set -e

# Display help message
show_help() {
    echo "Usage: ./scripts/run_dbt.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help         Show this help message"
    echo "  -e, --env ENV      Environment to run in (default: dev)"
    echo "  -m, --models NAME  Specific models to run (comma-separated, no spaces)"
    echo "  -t, --test         Run tests after models"
    echo "  -d, --docs         Generate documentation"
    echo "  -f, --full         Run full process (deps, run, test, docs)"
    echo "  -c, --clean        Run 'dbt clean' before other commands"
    echo "  -s, --seed         Run 'dbt seed' to load seed files"
    echo ""
    echo "Examples:"
    echo "  ./scripts/run_dbt.sh -f                     # Run full process"
    echo "  ./scripts/run_dbt.sh -m dim_location        # Run only dim_location model"
    echo "  ./scripts/run_dbt.sh -m 'marts.*'           # Run all models in marts directory"
    echo "  ./scripts/run_dbt.sh -e prod -t            # Run in prod environment with tests"
    echo "  ./scripts/run_dbt.sh -c -s -m stg_sales     # Clean, seed, then run stg_sales"
}

# Default values
ENV="dev"
MODELS="all"
RUN_TESTS=false
GENERATE_DOCS=false
RUN_DEPS=false
RUN_CLEAN=false
RUN_SEED=false

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
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Check if dbt is installed
if ! command -v dbt &> /dev/null; then
    echo "Error: dbt is not installed or not in your PATH"
    echo "Please install dbt (pip install dbt-core dbt-postgres) or activate your virtual environment"
    exit 1
fi

# Navigate to the project root directory (one level up from scripts/)
PROJECT_ROOT="$(dirname "$(dirname "$0")")"

# Navigate to the dbt project directory
cd "$PROJECT_ROOT/dbt_transform" || {
    echo "Error: Could not find the dbt_transform directory"
    exit 1
}

echo "🚀 Starting dbt workflow in environment: $ENV"
echo "----------------------------------------"

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
    echo "✅ Documentation generated. Run 'dbt docs serve' to view it in your browser."
fi

echo "----------------------------------------"
echo "✨ dbt workflow completed successfully! ✨"