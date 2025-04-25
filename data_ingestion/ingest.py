import os
import sys
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine
from .utils import get_db_connection_string, configure_logger

logger = configure_logger(__name__)


class CleanData:
    """
    A class for handling different types of data cleaning operations.
    Each method handles a specific type of cleaning.
    """
    
    @staticmethod
    def handle_missing_values(df):
        """Handle missing values in the DataFrame."""
        logger.info("Cleaning: Handling missing values...")
        df_cleaned = df.copy()
        df_cleaned['Location'] = df_cleaned['Location'].fillna('Unknown')
        return df_cleaned
    
    @staticmethod
    def standardize_data_types(df):
        """Convert all columns to strings for the raw layer."""
        logger.info("Cleaning: Standardizing data types...")
        df_cleaned = df.copy()
        for col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].astype(str)
        return df_cleaned
    
    @staticmethod
    def remove_whitespace_values(df):
        """Replace whitespace-only values with None."""
        logger.info("Cleaning: Removing whitespace values...")
        df_cleaned = df.copy()
        df_cleaned = df_cleaned.replace(' ', None)
        return df_cleaned
    
    @staticmethod
    def clean_price_values(df):
        """Clean price values by removing currency symbols."""
        logger.info("Cleaning: Cleaning price values...")
        df_cleaned = df.copy()
        
        # Handle price with currency notation
        df_cleaned.loc[df_cleaned['Price'].str.contains('USD', na=False), 'Price'] = \
            df_cleaned.loc[df_cleaned['Price'].str.contains('USD', na=False), 'Price'].str.replace('USD', '')
        
        # Strip whitespace
        df_cleaned['Price'] = df_cleaned['Price'].str.strip()
        
        return df_cleaned
    
    @staticmethod
    def clean_date_values(df):
        """Standardize date formats."""
        logger.info("Cleaning: Standardizing date formats...")
        df_cleaned = df.copy()
        
        # Convert different date formats to ISO format
        for idx, date_str in enumerate(df_cleaned['Date']):
            try:
                if isinstance(date_str, str) and date_str:
                    if '/' in date_str:
                        date_obj = datetime.strptime(date_str, '%Y/%m/%d')
                        df_cleaned.at[idx, 'Date'] = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                # Leave invalid dates as is - they'll be caught in validation
                pass
                
        return df_cleaned
    
    @classmethod
    def apply_all_cleaners(cls, df):
        """Apply all cleaning methods in sequence."""
        logger.info("Starting comprehensive data cleaning...")
        
        df_result = df.copy()
        df_result = cls.handle_missing_values(df_result)
        df_result = cls.standardize_data_types(df_result)
        df_result = cls.remove_whitespace_values(df_result)
        df_result = cls.clean_price_values(df_result)
        df_result = cls.clean_date_values(df_result)
        
        logger.info(f"Comprehensive data cleaning complete. Processed {len(df_result)} rows.")
        return df_result


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
