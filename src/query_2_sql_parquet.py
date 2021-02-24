from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query 2 - SQL, Parquet").getOrCreate()

df_ratings = spark.read.parquet(
    "hdfs://master:9000/movie_data/ratings.parquet")

# Create Table name for SQL queries
df_ratings.registerTempTable("ratings")

# Count Distinct Users
count_distinct_users = """
                SELECT COUNT(DISTINCT(userId)) AS usersCount
                FROM ratings
"""

# Unique users with Avg Rating > 3
over3_rating_users = """
                SELECT COUNT(*) AS usersCountOver3
                FROM (
                    SELECT COUNT(userId)
                    FROM ratings
                    GROUP BY userId
                    HAVING AVG(rating) > 3.0)
"""

q2_string = """
                SELECT (usersCountOver3/usersCount)*100 AS percentResult
                FROM (""" + count_distinct_users + """) 
                    CROSS JOIN
                     (""" + over3_rating_users + """)
"""
df_q2 = spark.sql(q2_string)
df_q2.show(df_q2.count(), truncate=False)
