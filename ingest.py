import os
import sys
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine
from utils import get_db_connection_string, configure_logger

logger = configure_logger(__name__)


def clean_data(df):
    """
    Clean the data by handling missing values and correcting data types.
    """
    logger.info("Cleaning data...")
    cleaned_df = df.copy()
    
    # Handle missing values
    cleaned_df['Location'] = cleaned_df['Location'].fillna('Unknown')
    
    # Keep all columns as strings for the raw layer (will be properly transformed in later stages)
    for col in cleaned_df.columns:
        cleaned_df[col] = cleaned_df[col].astype(str)
    
    # Set whitespace to None
    cleaned_df = cleaned_df.replace(' ', None)
    
    logger.info(f"Data cleaning complete. {len(cleaned_df)} rows processed.")
    return cleaned_df

def detect_duplicates(df, key_columns=['SaleID']):
    """
    Detect duplicate records based on specified key columns.
    """
    logger.info(f"Checking for duplicate records based on {key_columns}...")
    
    # Get count of duplicates
    duplicate_count = df.duplicated(subset=key_columns, keep='first').sum()
    
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate records")
        # Remove duplicates, keeping the first occurrence
        df = df.drop_duplicates(subset=key_columns, keep='first')
        logger.info(f"Removed duplicates. {len(df)} records remaining.")
    else:
        logger.info("No duplicates found.")
        
    return df

def validate_data(df):
    """
    Validate data quality and log any issues.
    """
    logger.info("Validating data...")
    
    # Check for invalid dates
    invalid_dates = []
    for idx, date_str in enumerate(df['Date']):
        try:
            # Try to parse the date
            if isinstance(date_str, str) and date_str:
                # Handle different date formats
                if '/' in date_str:
                    datetime.strptime(date_str, '%Y/%m/%d')
                else:
                    datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            invalid_dates.append((idx, date_str))
    
    if invalid_dates:
        logger.warning(f"Found {len(invalid_dates)} invalid date formats")
        for idx, date_val in invalid_dates:
            logger.warning(f"Row {idx}: Invalid date '{date_val}'")
    
    # Check for invalid quantities (negative or non-numeric)
    df_copy = df.copy()
    try:
        invalid_qty = df_copy[df_copy['Quantity'].apply(
            lambda x: not x.isdigit() or (x.startswith('-') and x[1:].isdigit())
        )]
        if not invalid_qty.empty:
            logger.warning(f"Found {len(invalid_qty)} records with invalid quantity values")
            logger.warning(f"Found invalid quantity values in rows: {invalid_qty.index.tolist()}")
    except:
        logger.warning("Could not validate quantity values")
    
    # Check for invalid prices (should be numeric)
    try:
        # Try to identify price values with currency symbols or other non-numeric characters
        df_copy['PriceCheck'] = df_copy['Price'].str.replace('-', '')
        invalid_price = df_copy[df_copy['PriceCheck'].apply(
            lambda x: not x.replace('.', '').isdigit()
        )]
        if not invalid_price.empty:
            logger.warning(f"Found {len(invalid_price)} records with invalid price formats")
            logger.warning(f"Found invalid price values in rows: {invalid_price.index.tolist()}")
    except:
        logger.warning("Could not validate price values")
    
    logger.info("Data validation complete.")
    return df

def load_to_raw(df, connection_string, file_path, table_name='sales'):
    """
    Load data to the raw schema in the database.
    """
    logger.info(f"Loading data to raw schema from {file_path}...")
    
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Add metadata columns
    batch_id = str(uuid.uuid4())
    df['inserted_at'] = datetime.now()
    df['batch_id'] = batch_id
    df['source_file'] = os.path.basename(file_path)
    
    # Write to PostgreSQL
    try:
        df.to_sql(table_name, engine, schema='raw', if_exists='append', index=False)
        logger.info(f"Successfully loaded {len(df)} records to raw.{table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"Error loading data to database: {str(e)}")
        raise

def main(file_path):
    """
    Main function to process and load data.
    """
    logger.info(f"Starting data ingestion process for {file_path}")
    
    try:
        # Read the CSV file
        logger.info(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read {len(df)} records from {file_path}")
        
        # Clean and validate data
        cleaned_df = clean_data(df)
        validated_df = validate_data(cleaned_df)
        
        # Check for duplicates
        deduplicated_df = detect_duplicates(validated_df)
        
        # Get database connection string
        connection_string = get_db_connection_string()
        
        # Load to raw schema
        records_loaded = load_to_raw(deduplicated_df, connection_string, file_path)
        
        logger.info(f"Data ingestion complete. {records_loaded} records processed.")
        return records_loaded
        
    except Exception as e:
        logger.error(f"Error in data ingestion process: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python ingest.py <path_to_csv_file>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    try:
        main(file_path)
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        sys.exit(1)
