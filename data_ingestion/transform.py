import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import text, create_engine
from .utils import configure_logger, get_db_connection_string

logger = configure_logger(__name__)


def create_transformed_schema(engine):
    """
    Create the transformed schema and tables.
    """
    logger.info("Creating transformed schema and tables...")
    
    create_tables_sql = """
    -- Create transformed tables
    CREATE SCHEMA IF NOT EXISTS transformed;
    
    -- Dimension tables
    CREATE TABLE IF NOT EXISTS transformed.dim_product (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        brand VARCHAR(100),
        category VARCHAR(100),
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS transformed.dim_retailer (
        retailer_id INTEGER PRIMARY KEY,
        retailer_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS transformed.dim_location (
        location_id SERIAL PRIMARY KEY,
        location_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS transformed.dim_channel (
        channel_id SERIAL PRIMARY KEY,
        channel_name VARCHAR(50) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS transformed.dim_date (
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
    CREATE TABLE IF NOT EXISTS transformed.fact_sales (
        sale_id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        retailer_id INTEGER NOT NULL,
        location_id INTEGER,
        channel_id INTEGER NOT NULL,
        date_id DATE NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(10, 2) NOT NULL,
        total_amount NUMERIC(12, 2) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES transformed.dim_product (product_id),
        FOREIGN KEY (retailer_id) REFERENCES transformed.dim_retailer (retailer_id),
        FOREIGN KEY (location_id) REFERENCES transformed.dim_location (location_id),
        FOREIGN KEY (channel_id) REFERENCES transformed.dim_channel (channel_id),
        FOREIGN KEY (date_id) REFERENCES transformed.dim_date (date_id)
    );
    
    -- Create indexes for better performance
    CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON transformed.fact_sales(product_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_retailer_id ON transformed.fact_sales(retailer_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_date_id ON transformed.fact_sales(date_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_channel_id ON transformed.fact_sales(channel_id);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_tables_sql))
        logger.info("Successfully created transformed schema and tables")
    except Exception as e:
        logger.error(f"Error creating transformed schema and tables: {str(e)}")
        raise

def populate_dim_product(engine):
    """
    Populate the product dimension table.
    """
    logger.info("Populating product dimension table...")
    
    # Extract unique products from raw data
    query = """
    SELECT DISTINCT "ProductID", "ProductName", "Brand", "Category" 
    FROM raw.sales 
    WHERE "ProductID" IS NOT NULL
    """
    
    try:
        # Get unique products
        with engine.begin() as conn:
            result = conn.execute(text(query))
            products = [dict(zip(["product_id", "product_name", "brand", "category"], row)) for row in result]
        
        # Insert products into dimension table
        product_ids = {}
        for product in products:
            # Convert product_id to integer if possible
            try:
                product_id = int(product["product_id"])
            except ValueError:
                logger.warning(f"Non-integer ProductID: {product['product_id']}, skipping")
                continue
            
            # Check if product already exists
            check_query = """
            SELECT product_id FROM transformed.dim_product WHERE product_id = :product_id
            """
            
            with engine.begin() as conn:
                result = conn.execute(text(check_query), {"product_id": product_id})
                row = result.fetchone()
                
                if row:
                    # Update existing product if needed
                    update_query = """
                    UPDATE transformed.dim_product
                    SET product_name = :product_name,
                        brand = :brand,
                        category = :category,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE product_id = :product_id
                    """
                    conn.execute(text(update_query), {
                        "product_id": product_id,
                        "product_name": product["product_name"],
                        "brand": product["brand"],
                        "category": product["category"]
                    })
                else:
                    # Insert new product
                    insert_query = """
                    INSERT INTO transformed.dim_product (product_id, product_name, brand, category)
                    VALUES (:product_id, :product_name, :brand, :category)
                    """
                    conn.execute(text(insert_query), {
                        "product_id": product_id,
                        "product_name": product["product_name"],
                        "brand": product["brand"],
                        "category": product["category"]
                    })
                product_ids[product["product_id"]] = product_id
        
        logger.info(f"Successfully populated product dimension table with {len(product_ids)} products")
        return product_ids
    except Exception as e:
        logger.error(f"Error populating product dimension table: {str(e)}")
        raise

def populate_dim_retailer(engine):
    """
    Populate the retailer dimension table.
    """
    logger.info("Populating retailer dimension table...")
    
    # Extract unique retailers from raw data
    query = """
    SELECT DISTINCT "RetailerID", "RetailerName" 
    FROM raw.sales 
    WHERE "RetailerID" IS NOT NULL
    """
    
    try:
        # Get unique retailers
        with engine.begin() as conn:
            result = conn.execute(text(query))
            retailers = [dict(zip(["retailer_id", "retailer_name"], row)) for row in result]
        
        # Insert retailers into dimension table
        retailer_ids = {}
        for retailer in retailers:
            # Convert retailer_id to integer if possible
            try:
                retailer_id = int(retailer["retailer_id"])
            except ValueError:
                logger.warning(f"Non-integer RetailerID: {retailer['retailer_id']}, skipping")
                continue
            
            # Check if retailer already exists
            check_query = """
            SELECT retailer_id FROM transformed.dim_retailer WHERE retailer_id = :retailer_id
            """
            
            with engine.begin() as conn:
                result = conn.execute(text(check_query), {"retailer_id": retailer_id})
                row = result.fetchone()
                
                if row:
                    # Update existing retailer if needed
                    update_query = """
                    UPDATE transformed.dim_retailer
                    SET retailer_name = :retailer_name,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE retailer_id = :retailer_id
                    """
                    conn.execute(text(update_query), {
                        "retailer_id": retailer_id,
                        "retailer_name": retailer["retailer_name"]
                    })
                else:
                    # Insert new retailer
                    insert_query = """
                    INSERT INTO transformed.dim_retailer (retailer_id, retailer_name)
                    VALUES (:retailer_id, :retailer_name)
                    """
                    conn.execute(text(insert_query), {
                        "retailer_id": retailer_id,
                        "retailer_name": retailer["retailer_name"]
                    })
                retailer_ids[retailer["retailer_id"]] = retailer_id
        
        logger.info(f"Successfully populated retailer dimension table with {len(retailer_ids)} retailers")
        return retailer_ids
    except Exception as e:
        logger.error(f"Error populating retailer dimension table: {str(e)}")
        raise

def populate_dim_location(engine):
    """
    Populate the location dimension table.
    """
    logger.info("Populating location dimension table...")
    
    # Extract unique locations from raw data
    query = """
    SELECT DISTINCT "Location" FROM raw.sales WHERE "Location" IS NOT NULL AND "Location" != ''
    """
    
    try:
        # Get unique locations
        with engine.begin() as conn:
            result = conn.execute(text(query))
            locations = [row[0] for row in result]
        
        # Insert locations into dimension table
        location_ids = {}
        
        for location in locations:
            # Check if location already exists
            check_query = """
            SELECT location_id FROM transformed.dim_location WHERE location_name = :location
            """
            
            with engine.begin() as conn:
                result = conn.execute(text(check_query), {"location": location})
                row = result.fetchone()
                
                if row:
                    location_id = row[0]
                else:
                    # Insert new location
                    insert_query = """
                    INSERT INTO transformed.dim_location (location_name)
                    VALUES (:location)
                    RETURNING location_id
                    """
                    result = conn.execute(text(insert_query), {"location": location})
                    location_id = result.fetchone()[0]
                
                location_ids[location] = location_id
        
        logger.info(f"Successfully populated location dimension table with {len(location_ids)} locations")
        return location_ids
    except Exception as e:
        logger.error(f"Error populating location dimension table: {str(e)}")
        raise

def populate_dim_channel(engine):
    """
    Populate the channel dimension table.
    """
    logger.info("Populating channel dimension table...")
    
    # Extract unique channels from raw data
    query = """
    SELECT DISTINCT "Channel" FROM raw.sales WHERE "Channel" IS NOT NULL
    """
    
    try:
        # Get unique channels
        with engine.begin() as conn:
            result = conn.execute(text(query))
            channels = [row[0] for row in result]
        
        # Insert channels into dimension table
        channel_ids = {}
        for channel in channels:
            # Check if channel already exists
            check_query = """
            SELECT channel_id FROM transformed.dim_channel WHERE channel_name = :channel
            """
            
            with engine.begin() as conn:
                result = conn.execute(text(check_query), {"channel": channel})
                row = result.fetchone()
                
                if row:
                    channel_id = row[0]
                else:
                    # Insert new channel
                    insert_query = """
                    INSERT INTO transformed.dim_channel (channel_name)
                    VALUES (:channel)
                    RETURNING channel_id
                    """
                    result = conn.execute(text(insert_query), {"channel": channel})
                    channel_id = result.fetchone()[0]
                
                channel_ids[channel] = channel_id
        
        logger.info(f"Successfully populated channel dimension table with {len(channel_ids)} channels")
        return channel_ids
    except Exception as e:
        logger.error(f"Error populating channel dimension table: {str(e)}")
        raise

def populate_dim_date(engine, start_date='2024-01-01', end_date='2025-12-31'):
    """
    Populate the date dimension table.
    """
    logger.info(f"Populating date dimension table from {start_date} to {end_date}...")
    
    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date)
    
    try:
        # Create a list to hold all date records
        date_records = []
        for date in date_range:
            date_id = date.strftime('%Y-%m-%d')
            day = date.day
            month = date.month
            year = date.year
            quarter = (month - 1) // 3 + 1
            day_of_week = date.dayofweek
            day_name = date.strftime('%A')
            month_name = date.strftime('%B')
            is_weekend = day_of_week >= 5
            
            date_records.append({
                "date_id": date_id,
                "day": day,
                "month": month,
                "year": year,
                "quarter": quarter,
                "day_of_week": day_of_week,
                "day_name": day_name,
                "month_name": month_name,
                "is_weekend": is_weekend
            })
        
        # Insert dates into dimension table
        with engine.begin() as conn:
            for record in date_records:
                # Check if date already exists
                check_query = """
                SELECT date_id FROM transformed.dim_date WHERE date_id = :date_id
                """
                result = conn.execute(text(check_query), {"date_id": record["date_id"]})
                if not result.fetchone():
                    # Insert new date
                    insert_query = """
                    INSERT INTO transformed.dim_date (
                        date_id, day, month, year, quarter, 
                        day_of_week, day_name, month_name, is_weekend
                    )
                    VALUES (
                        :date_id, :day, :month, :year, :quarter,
                        :day_of_week, :day_name, :month_name, :is_weekend
                    )
                    """
                    conn.execute(text(insert_query), record)
        
        logger.info(f"Successfully populated date dimension table with {len(date_records)} dates")
        return [record["date_id"] for record in date_records]
    except Exception as e:
        logger.error(f"Error populating date dimension table: {str(e)}")
        raise

def process_sales_data(engine):
    """
    Process and transform sales data from raw to fact table.
    """
    
    try:
        # Get dimension lookups
        channel_ids = populate_dim_channel(engine)
        location_ids = populate_dim_location(engine)
        product_ids = populate_dim_product(engine)
        retailer_ids = populate_dim_retailer(engine)
        date_ids = populate_dim_date(engine)
        
        # Query to get raw sales data
        query = """
        SELECT "SaleID", "ProductID", "RetailerID", "Channel", "Location", 
               "Quantity", "Price", "Date"
        FROM raw.sales
        WHERE "SaleID" NOT IN (
            SELECT sale_id::VARCHAR FROM transformed.fact_sales
        )
        ORDER BY "SaleID" ASC
        """
        
        with engine.begin() as conn:
            # Count total records to process
            count_query = """
            SELECT COUNT(*) FROM raw.sales
            WHERE "SaleID" NOT IN (
                SELECT sale_id::VARCHAR FROM transformed.fact_sales
            )
            """
            result = conn.execute(text(count_query))
            total_records = result.fetchone()[0]
            logger.info(f"Found {total_records} new sales records to process")
            
            if total_records == 0:
                logger.info("No new records to process")
                return 0
            
            result = conn.execute(text(query))
            sales = [dict(zip(result.keys(), row)) for row in result]
            
            # Transform data
            logger.info(f"Processing {len(sales)} sales records")
            processed_count = 0
            fact_records = []
            for sale in sales:
                try:
                    # Clean and transform data
                    sale_id = int(sale["SaleID"])
                    
                    # Skip if product or retailer not in dimension tables
                    if sale["ProductID"] not in product_ids or sale["RetailerID"] not in retailer_ids:
                        logger.warning(f"Skipping sale {sale_id}: Missing dimension references")
                        continue
                    
                    product_id = product_ids[sale["ProductID"]]
                    retailer_id = retailer_ids[sale["RetailerID"]]
                    
                    # Get channel ID
                    channel = sale["Channel"]
                    channel_id = channel_ids.get(channel)
                    if not channel_id:
                        logger.warning(f"Unknown channel '{channel}' for sale {sale_id}")
                        continue
                    
                    # Get location ID
                    location = sale["Location"]
                    location_id = location_ids.get(location)
                    if not location_id:
                        logger.warning(f"Unknown location '{location}' for sale {sale_id}")
                        continue
                    
                    # Parse quantity
                    try:
                        quantity = int(sale["Quantity"])
                        if quantity < 0:
                            logger.warning(f"Skipping sale {sale_id}: Negative quantity '{sale['Quantity']}'")
                            continue
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid quantity '{sale['Quantity']}' for sale {sale_id}")
                    except:
                        logger.error(f"Unexpected error parsing quantity for sale {sale_id}: {str(e)}")
                        continue
                    
                    # Parse price
                    try:
                        # Remove any currency symbols or text
                        price_str = str(sale["Price"]).replace('USD', '').strip()
                        unit_price = float(price_str) / quantity
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid price '{sale['Price']}' for sale {sale_id}")
                    except:
                        logger.error(f"Unexpected error parsing price for sale {sale_id}: {str(e)}")
                        continue
                    
                    total_amount = float(price_str)
                    
                    # Parse date
                    try:
                        # Handle different date formats
                        date_str = sale["Date"]
                        if '/' in date_str:
                            date_obj = datetime.strptime(date_str, '%Y/%m/%d')
                        else:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                        date_id = date_obj.strftime('%Y-%m-%d')
                        if date_id not in date_ids:
                            logger.warning(f"Date '{date_id}' not in date dimension for sale {sale_id}")
                            continue
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid date '{sale['Date']}' for sale {sale_id}, skipping")
                        continue
                    
                    # Create fact record
                    fact_record = {
                        "sale_id": sale_id,
                        "product_id": product_id,
                        "retailer_id": retailer_id,
                        "location_id": location_id,
                        "channel_id": channel_id,
                        "date_id": date_id,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "total_amount": total_amount
                    }
                    fact_records.append(fact_record)
                except Exception as e:
                    logger.error(f"Error processing sale {sale['SaleID']}: {str(e)}")
            
            # Insert fact records
            if fact_records:
                try:
                    insert_query = """
                    INSERT INTO transformed.fact_sales (
                        sale_id, product_id, retailer_id, location_id, 
                        channel_id, date_id, quantity, unit_price, total_amount
                    )
                    VALUES (
                        :sale_id, :product_id, :retailer_id, :location_id, 
                        :channel_id, :date_id, :quantity, :unit_price, :total_amount
                    )
                    ON CONFLICT (sale_id) DO NOTHING
                    """
                    # Use a new transaction to ensure atomicity
                    with engine.begin() as insert_conn:
                        insert_conn.execute(text(insert_query), fact_records)
                    
                    processed_count = len(fact_records)
                    logger.info(f"Inserted {len(fact_records)} records into fact_sales")
                except Exception as e:
                    logger.error(f"Error inserting: {str(e)}")
            
            logger.info(f"Successfully processed {processed_count} sales records")
            return processed_count
    except Exception as e:
        logger.error(f"Error processing sales data: {str(e)}")
        raise

def main():
    """
    Main function to transform and load data from raw to transformed schema.
    """
    logger.info("Starting data transformation process")
    
    try:
        # Get database connection string
        connection_string = get_db_connection_string()
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Create transformed schema and tables
        create_transformed_schema(engine)

        # Process sales data
        processed_count = process_sales_data(engine)
        
        logger.info(f"Data transformation complete. {processed_count} records processed.")
        return processed_count
        
    except Exception as e:
        logger.error(f"Error in data transformation process: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        sys.exit(1)
