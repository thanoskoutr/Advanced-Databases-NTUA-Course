from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType


spark = SparkSession.builder.appName("query 2 - SQL, CSV").getOrCreate()

# Create schema for Table
schema = StructType([
    StructField("userId", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", DoubleType()),
    StructField("timestamp", StringType()),
])

# Load CSV into dataframe
df_ratings = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .schema(schema) \
    .load("hdfs://master:9000/movie_data/ratings.csv")


# Create Table name for SQL queries
df_ratings.registerTempTable("ratings")

# Count Distinct Users
count_distinct_users = """
                SELECT COUNT(DISTINCT(userId)) as usersCount
                FROM ratings
"""

# Unique users with Avg Rating > 3
over3_rating_users = """
                SELECT COUNT(*) as usersCountOver3
                FROM (
                    SELECT COUNT(userId)
                    FROM ratings
                    GROUP BY userId
                    HAVING AVG(rating) > 3.0)
"""

q2_string = """
                SELECT (usersCountOver3/usersCount)*100 as percentResult
                FROM (""" + count_distinct_users + """) 
                    CROSS JOIN
                     (""" + over3_rating_users + """)
"""
df_q2 = spark.sql(q2_string)
df_q2.show(df_q2.count(), truncate=False)
