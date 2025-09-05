from pyspark.sql import SparkSession
from src.config import SPARK_CONFIG
import os

def extract_csv_to_parquet(csv_path: str, parquet_path: str, spark: SparkSession):
    spark = SparkSession.builder \
        .appName(SPARK_CONFIG["app_name"]) \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .getOrCreate()

    print("✅ Spark session created")

    if os.path.exists(parquet_path):
        print(f"📦 Parquet file already exists at {parquet_path}. Reading from it...")
        print("hello")
        df = spark.read.parquet(parquet_path)
        print("hello")
        
    else:
        print(f"📄 Reading CSV from {csv_path}")
        df = spark.read.option("header", True).csv(csv_path)

        print(f"💾 Writing to Parquet at {parquet_path}")
        df.write.mode("overwrite").parquet(parquet_path)

    return df
