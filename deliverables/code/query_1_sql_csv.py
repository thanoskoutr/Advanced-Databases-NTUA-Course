from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
import time
import sys

# Start counting execution time
start_time = time.time()

spark = SparkSession.builder.appName("query 1 - SQL, CSV").getOrCreate()

# Create schema for Table
schema = StructType([
    StructField("movieId", IntegerType()),
    StructField("title", StringType()),
    StructField("summary", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("duration", DoubleType()),
    StructField("cost", IntegerType()),
    StructField("revenue", IntegerType()),
    StructField("popularity", DoubleType()),
])

# Load CSV into dataframe
df_movies = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .schema(schema) \
    .load("hdfs://master:9000/movie_data/movies.csv")

# Create Table name for SQL queries
df_movies.registerTempTable("movies")

most_profit_by_year = """
                SELECT YEAR(timestamp) AS year, MAX(((revenue-cost)/cost)*100) AS profit
                FROM movies
                WHERE timestamp IS NOT NULL AND cost IS NOT NULL AND revenue IS NOT NULL
                    AND cost != 0 AND revenue != 0 
                    AND YEAR(timestamp) >= 2000
                GROUP BY YEAR(timestamp)
"""

q1_string = """
                SELECT YEAR(timestamp) AS year, title
                FROM movies AS m
                    INNER JOIN (""" + most_profit_by_year + """) AS g
                        ON YEAR(m.timestamp) = g.year
                WHERE ((m.revenue-m.cost)/m.cost)*100 = g.profit
                ORDER BY YEAR(timestamp) ASC
"""

# Result Dataframe from query 1
df_q1 = spark.sql(q1_string)

# Display all results in the Dataframe
df_q1.show(df_q1.count(), truncate=False)


# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
