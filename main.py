# from src.extract import extract_csv_to_parquet
# from src.transform import transform_initial_data, create_star_schema
# from src.load import load_to_postgres
# from src.logger import get_logger
# from pyspark.sql import SparkSession
# from src.config import SPARK_CONFIG
# import os

# os.environ['PYSPARK_PYTHON']=r'C:/Users/nilpanda/Desktop/Projects/EducationAnalytics/.venv3/Scripts/python.exe'

# logger = get_logger()

# def run_initial_etl():
#     try:
#         spark = SparkSession.builder \
#             .appName(SPARK_CONFIG["app_name"]) \
#             .master(SPARK_CONFIG["master"]) \
#             .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
#             .config("spark.python.worker.faulthandler.enabled", "true") \
#             .getOrCreate()

#         logger.info("🚀 Starting ETL pipeline")

#         # Step 1: Extract
#         df = extract_csv_to_parquet(
#             # csv_path="data/raw/students_dataset.csv",
#             csv_path="C:/Users/nilpanda/Desktop/Projects/EducationAnalytics/data/students_dataset.csv",
#             parquet_path="data/processed/students.parquet",
#             spark=spark
#         )

#         # Step 2: Transform
#         transformed_df = transform_initial_data(df)

#         print("transformed_df - ", transformed_df)
#         transformed_df.write.mode("overwrite").parquet("data/processed/students_cleaned.parquet")
#         print("transformed_df is overwritten")
#         logger.info("✅ Data transformed and saved to cleaned Parquet")

#         # # Step 3: Star Schema
#         print("adding star schema")
#         dim_year, dim_location, dim_institute, fact_table = create_star_schema(transformed_df, spark=spark)
#         logger.info("✅ Star schema tables created")

#         # # Step 4: Load to PostgreSQL
#         print("adding data")
#         logger.info("📥 Starting data load to PostgreSQL")

#         load_to_postgres("dim_year", dim_year)
#         load_to_postgres("dim_location", dim_location)
#         load_to_postgres("dim_institute", dim_institute)
#         load_to_postgres("fact_student_distribution", fact_table)

#         logger.info("✅ All tables loaded into PostgreSQL")


#         print("adding data finished")

#     except Exception as e:
#         logger.error(f"❌ ETL pipeline failed: {e}")

#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     run_initial_etl()


from src.extract import extract_csv_to_parquet
from src.transform import transform_initial_data, create_star_schema
from src.load import load_to_postgres
from src.logger import get_logger
from pyspark.sql import SparkSession
from src.config import SPARK_CONFIG
import os

os.environ['PYSPARK_PYTHON'] = r'C:/Users/nilpanda/Desktop/Projects/EducationAnalytics/.venv3/Scripts/python.exe'

logger = get_logger()

def run_initial_etl():
    spark = None  # ✅ Declare spark outside try block

    try:
        spark = SparkSession.builder \
            .appName(SPARK_CONFIG["app_name"]) \
            .master(SPARK_CONFIG["master"]) \
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate()

        logger.info("🚀 Starting ETL pipeline")

        # Step 1: Extract
        df = extract_csv_to_parquet(
            csv_path="C:/Users/nilpanda/Desktop/Projects/EducationAnalytics/data/students_dataset.csv",
            parquet_path="data/processed/students.parquet",
            spark=spark
        )

        # Step 2: Transform
        transformed_df = transform_initial_data(df)
        transformed_df.write.mode("overwrite").parquet("data/processed/students_cleaned.parquet")
        logger.info("✅ Data transformed and saved to cleaned Parquet")

        # Step 3: Star Schema
        dim_year, dim_location, dim_institute, fact_table = create_star_schema(transformed_df, spark=spark)
        logger.info("✅ Star schema tables created")

        # Step 4: Load to PostgreSQL
        logger.info("📥 Starting data load to PostgreSQL")
        load_to_postgres("dim_year", dim_year)
        load_to_postgres("dim_location", dim_location)
        load_to_postgres("dim_institute", dim_institute)
        load_to_postgres("fact_student_distribution", fact_table)
        logger.info("✅ All tables loaded into PostgreSQL")

    except Exception as e:
        logger.error(f"❌ ETL pipeline failed: {e}")

    finally:
        if spark:
            spark.stop()  # ✅ Only stop if spark was successfully created

if __name__ == "__main__":
    run_initial_etl()