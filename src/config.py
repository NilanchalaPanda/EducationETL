import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Spark Configuration
SPARK_CONFIG = {
    "app_name": os.getenv("SPARK_APP_NAME", "DefaultSparkApp"),
    "master": os.getenv("SPARK_MASTER", "local[*]")
}

# PostgreSQL Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "etl"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "root")
}