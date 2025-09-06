# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, mean, when, udf, monotonically_increasing_id
# from pyspark.sql.types import StringType
# import uuid
# from pyspark.sql import SparkSession

# def transform_initial_data(df: DataFrame) -> DataFrame:
#     try:
#         # Normalize column names
#         for col_name in df.columns:
#             df = df.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))

#         # Drop missing values
#         df_clean = df.dropna()

#         # Add UUID-based student_id
#         uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
#         df_clean = df_clean.withColumn("student_id", uuid_udf())

#         # Cast scores to integers
#         score_cols = ["midterm_score", "final_score", "projects_score", "assignments_avg", "participation_score", "quizzes_avg"]
#         for col_name in score_cols:
#             df_clean = df_clean.withColumn(col_name, col(col_name).cast("double").cast("int"))

#         # Compute average score
#         df_clean = df_clean.withColumn(
#             "average_score",
#             (col("midterm_score") + col("final_score") + col("projects_score") +
#              col("assignments_avg") + col("participation_score") + col("quizzes_avg")) / 6
#         )

#         # Assign grades
#         df_clean = df_clean.withColumn(
#             "grade",
#             when(col("average_score") >= 90, "A")
#             .when(col("average_score") >= 80, "B")
#             .when(col("average_score") >= 70, "C")
#             .when(col("average_score") >= 60, "D")
#             .otherwise("F")
#         )

#         return df_clean

#     except Exception as e:
#         from src.logger import get_logger
#         logger = get_logger()
#         logger.error(f"Transformation failed: {e}")
#         raise


# def create_star_schema(df: DataFrame, spark: SparkSession):
#     # DIMENSION: Year
#     dim_year = df.select(col("midterm_score")) \
#                  .withColumn("year", col("midterm_score") * 0 + 2025) \
#                  .drop("midterm_score") \
#                  .dropDuplicates() \
#                  .withColumn("year_id", monotonically_increasing_id())

#     # DIMENSION: Location
#     dim_location = df.select("college_location").dropDuplicates() \
#                      .withColumn("location_id", monotonically_increasing_id())

#     # DIMENSION: Institute
#     dim_institute = df.select("college_name").dropDuplicates() \
#                       .withColumn("institute_id", monotonically_increasing_id())

#     # Broadcast dimensions for optimized joins
#     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # disable auto
#     dim_location_b = spark.sparkContext.broadcast(dim_location.collect())
#     dim_institute_b = spark.sparkContext.broadcast(dim_institute.collect())

#     # Convert broadcasted dimensions back to DataFrames
#     dim_location_df = spark.createDataFrame(dim_location_b.value, dim_location.schema)
#     dim_institute_df = spark.createDataFrame(dim_institute_b.value, dim_institute.schema)

#     # FACT TABLE: Join with dimensions
#     fact_table = df.join(dim_institute_df, on="college_name", how="left") \
#                    .join(dim_location_df, on="college_location", how="left") \
#                    .select(
#                        "student_id", "age", "department", "midterm_score", "final_score",
#                        "attendance", "average_score", "grade", "institute_id", "location_id"
#                    ) \
#                    .withColumn("fact_id", monotonically_increasing_id())

#     return dim_year, dim_location_df, dim_institute_df, fact_table


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, udf, trim, lower, monotonically_increasing_id
from pyspark.sql.types import StringType
import uuid

def transform_initial_data(df: DataFrame) -> DataFrame:
    try:
        # Normalize column names
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))

        # Drop missing values
        df_clean = df.dropna()

        # Add UUID-based student_id
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        df_clean = df_clean.withColumn("student_id", uuid_udf())

        # Cast scores to integers
        score_cols = ["midterm_score", "final_score", "projects_score", "assignments_avg", "participation_score", "quizzes_avg"]
        for col_name in score_cols:
            df_clean = df_clean.withColumn(col_name, col(col_name).cast("double").cast("int"))

        # Compute average score
        df_clean = df_clean.withColumn(
            "average_score",
            (col("midterm_score") + col("final_score") + col("projects_score") +
             col("assignments_avg") + col("participation_score") + col("quizzes_avg")) / 6
        )

        # Assign grades
        df_clean = df_clean.withColumn(
            "grade",
            when(col("average_score") >= 90, "A")
            .when(col("average_score") >= 80, "B")
            .when(col("average_score") >= 70, "C")
            .when(col("average_score") >= 60, "D")
            .otherwise("F")
        )

        # Normalize join keys
        df_clean = df_clean.withColumn("college_name", trim(lower(col("college_name"))))
        df_clean = df_clean.withColumn("college_location", trim(lower(col("college_location"))))

        return df_clean

    except Exception as e:
        from src.logger import get_logger
        logger = get_logger()
        logger.error(f"Transformation failed: {e}")
        raise

def create_star_schema(df: DataFrame, spark: SparkSession):
    # Create normalized dimension tables
    dim_location = df.select(trim(lower(col("college_location"))).alias("college_location")).dropDuplicates()
    dim_location = dim_location.withColumn("location_id", monotonically_increasing_id())

    dim_institute = df.select(trim(lower(col("college_name"))).alias("college_name")).dropDuplicates()
    dim_institute = dim_institute.withColumn("institute_id", monotonically_increasing_id())

    dim_year = df.select(col("midterm_score")) \
                 .withColumn("year", col("midterm_score") * 0 + 2025) \
                 .drop("midterm_score") \
                 .dropDuplicates() \
                 .withColumn("year_id", monotonically_increasing_id())

    # Join dimensions to fact table
    fact_table = df.join(dim_institute, on="college_name", how="left") \
                   .join(dim_location, on="college_location", how="left") \
                   .select(
                       "student_id", "age", "department", "midterm_score", "final_score",
                       "attendance", "average_score", "grade", "institute_id", "location_id", "college_name", "college_location"
                   ) \
                   .withColumn("fact_id", monotonically_increasing_id())

    return dim_year, dim_location, dim_institute, fact_table
