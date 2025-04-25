import os
import sys
import logging
from dotenv import load_dotenv

load_dotenv()


def configure_logger(name):
    """
    Configure and return a logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():  # Avoid duplicate handlers
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s'
            )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler("data_processing.log")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

def get_db_connection_string():
    """
    Get the database connection string from environment variables or use default
    """
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "mysecretpassword")
    database = os.getenv("DB_NAME", "sales")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
