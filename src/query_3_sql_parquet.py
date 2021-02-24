from pyspark.sql import SparkSession
import time
import sys

spark = SparkSession.builder.appName("query 3 - SQL, Parquet").getOrCreate()

# Start counting execution time
start_time = time.time()

df_movie_genres = spark.read.parquet(
    "hdfs://master:9000/movie_data/movie_genres.parquet")

df_ratings = spark.read.parquet(
    "hdfs://master:9000/movie_data/ratings.parquet")

df_movie_genres.registerTempTable("movie_genres")
df_ratings.registerTempTable("ratings")

joined_table = """
    SELECT movie_genres.movieId, genre, rating
    FROM movie_genres INNER JOIN ratings ON movie_genres.movieId = ratings.movieId
"""
group_by_genres_and_movieID = """
    SELECT movieId, genre, AVG(rating) AS AverageRating
    FROM (""" + joined_table + """)
    GROUP BY movieId, genre
"""

avg_ratings_per_genre = """
    SELECT genre, AVG(AverageRating) AS AverageRatingPerGenre, COUNT(*) AS Count
    FROM (""" + group_by_genres_and_movieID + """)
    GROUP BY genre
"""
q3 = spark.sql(avg_ratings_per_genre)
q3.show(q3.count(), truncate=False)


# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
