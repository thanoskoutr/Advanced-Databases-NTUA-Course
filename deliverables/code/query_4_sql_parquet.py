from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import time
import sys

# Start counting execution time
start_time = time.time()


def return_5years_period(year):
    if (year <= 2004):
        return "2000-2004"
    elif (year <= 2009):
        return "2005-2009"
    elif (year <= 2014):
        return "2010-2014"
    elif (year <= 2019):
        return "2015-2019"


spark = SparkSession.builder.appName("query 4 - SQL, Parquet").getOrCreate()

df_movies = spark.read.parquet(
    "hdfs://master:9000/movie_data/movies.parquet")

df_movie_genres = spark.read.parquet(
    "hdfs://master:9000/movie_data/movie_genres.parquet")

# Create Table name for SQL queries
df_movies.registerTempTable("movies")

# Create Table name for SQL queries
df_movie_genres.registerTempTable("movie_genres")


spark.udf.register("return_5years_period", return_5years_period)


movies_filtered = """
        SELECT movieId, LENGTH(summary) AS summaryLength, YEAR(timestamp) AS year
        FROM movies
        WHERE timestamp IS NOT NULL AND 
            YEAR(timestamp) >= 2000 AND 
            YEAR(timestamp) <= 2019
"""
# df_movies_filtered = spark.sql(movies_filtered)
# df_movies_filtered.show(15, truncate=False)

movie_genres_filtered = """
        SELECT movieId
        FROM movie_genres
        WHERE genre = "Drama"
"""
# df_movie_genres_filtered = spark.sql(movie_genres_filtered)
# df_movie_genres_filtered.show(15, truncate=False)

joined_table = """
        SELECT m.movieId, summaryLength, return_5years_period(year) AS 5yearsPeriod
        FROM (""" + movies_filtered + """) AS m 
            INNER JOIN 
             (""" + movie_genres_filtered + """) AS mg 
            ON m.movieId = mg.movieId
"""
# df_joined_table = spark.sql(joined_table)
# df_joined_table.show(15, truncate=False)


avg_summary_len_by_5years = """
        SELECT 5yearsPeriod, AVG(summaryLength) AS AvgSummaryLength
        FROM (""" + joined_table + """)
        GROUP BY 5yearsPeriod
        ORDER BY 5yearsPeriod
"""
df_q4 = spark.sql(avg_summary_len_by_5years)
df_q4.show(df_q4.count(), truncate=False)


# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
