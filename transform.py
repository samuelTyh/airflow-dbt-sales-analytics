from sqlalchemy import text, create_engine
from utils import configure_logger, get_db_connection_string

logger = configure_logger(__name__)


def create_staging_schema(engine):
    """
    Create the staging schema and tables.
    """
    logger.info("Creating staging schema and tables...")
    
    create_tables_sql = """
    -- Create staging tables
    CREATE SCHEMA IF NOT EXISTS staging;
    
    -- Dimension tables
    CREATE TABLE IF NOT EXISTS staging.dim_product (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        brand VARCHAR(100),
        category VARCHAR(100),
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS staging.dim_retailer (
        retailer_id INTEGER PRIMARY KEY,
        retailer_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS staging.dim_location (
        location_id SERIAL PRIMARY KEY,
        location_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS staging.dim_channel (
        channel_id SERIAL PRIMARY KEY,
        channel_name VARCHAR(50) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS staging.dim_date (
        date_id DATE PRIMARY KEY,
        day INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        day_of_week INTEGER NOT NULL,
        day_name VARCHAR(10) NOT NULL,
        month_name VARCHAR(10) NOT NULL,
        is_weekend BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Fact table
    CREATE TABLE IF NOT EXISTS staging.fact_sales (
        sale_id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        retailer_id INTEGER NOT NULL,
        location_id INTEGER,
        channel_id INTEGER NOT NULL,
        date_id DATE NOT NULL,
        quantity INTEGER NOT NULL,
        price NUMERIC(10, 2) NOT NULL,
        total_amount NUMERIC(12, 2) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES staging.dim_product (product_id),
        FOREIGN KEY (retailer_id) REFERENCES staging.dim_retailer (retailer_id),
        FOREIGN KEY (location_id) REFERENCES staging.dim_location (location_id),
        FOREIGN KEY (channel_id) REFERENCES staging.dim_channel (channel_id),
        FOREIGN KEY (date_id) REFERENCES staging.dim_date (date_id)
    );
    
    -- Create indexes for better performance
    CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON staging.fact_sales(product_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_retailer_id ON staging.fact_sales(retailer_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_date_id ON staging.fact_sales(date_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_channel_id ON staging.fact_sales(channel_id);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_tables_sql))
            conn.commit()
        logger.info("Successfully created staging schema and tables")
    except Exception as e:
        logger.error(f"Error creating staging schema and tables: {str(e)}")
        raise

def main():
    """
    Main function to transform and load data from raw to staging schema.
    """
    logger.info("Starting data transformation process")
    
    try:
        # Get database connection string
        connection_string = get_db_connection_string()
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Create staging schema and tables
        create_staging_schema(engine)

        # Process sales data

        
    except Exception as e:
        raise

if __name__ == "__main__":
    main()
