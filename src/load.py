import pandas as pd
from sqlalchemy import create_engine
from src.config import DB_CONFIG
from src.logger import get_logger

logger = get_logger()

# def load_to_postgres(table_name: str, spark_df):
    # try:
    #     # Convert Spark DataFrame to Pandas
    #     pandas_df = spark_df.toPandas()

    #     print("DATA - ", pandas_df)

    #     # Create SQLAlchemy engine
    #     engine = create_engine(
    #         f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    #     )

    #     print(f"🔄 Loading {table_name}...")
    #     pandas_df.to_sql(table_name, engine, if_exists="replace", index=False)
    #     print(f"✅ Loaded {table_name} into PostgreSQL")
    #     logger.info(f"✅ Loaded {table_name} into PostgreSQL")

    # except Exception as e:
    #     print(f"❌ Failed to load {table_name}: {e}")
    #     logger.error(f"❌ Failed to load {table_name}: {e}")



def load_to_postgres(table_name: str, spark_df):
    try:
        # Convert Spark DataFrame to Pandas
        pandas_df = spark_df.toPandas()
        print("DATA - ", pandas_df)

        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        print(f"🔄 Loading {table_name}...")
        pandas_df.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"✅ Loaded {table_name} into PostgreSQL")
        logger.info(f"✅ Loaded {table_name} into PostgreSQL")

    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")
        logger.error(f"❌ Failed to load {table_name}: {e}")