from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
import time
import sys

# Start counting execution time
start_time = time.time()


spark = SparkSession.builder.appName("query 5 - SQL, CSV").getOrCreate()

# Create schema for Table movies
schema_movies = StructType([
    StructField("movieId", IntegerType()),
    StructField("title", StringType()),
    StructField("summary", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("duration", DoubleType()),
    StructField("cost", IntegerType()),
    StructField("revenue", IntegerType()),
    StructField("popularity", DoubleType()),
])

# Create schema for Table movie_genres
schema_movie_genres = StructType([
    StructField("movieId", IntegerType()),
    StructField("genre", StringType()),
])

# Create schema for Table ratings
schema_ratings = StructType([
    StructField("userId", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", DoubleType()),
    StructField("timestamp", StringType()),
])

# Load CSV into dataframe
df_movies = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .schema(schema_movies) \
    .load("hdfs://master:9000/movie_data/movies.csv")

# Load CSV into dataframe
df_movie_genres = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .schema(schema_movie_genres) \
    .load("hdfs://master:9000/movie_data/movie_genres.csv")

# Load CSV into dataframe
df_ratings = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .schema(schema_ratings) \
    .load("hdfs://master:9000/movie_data/ratings.csv")


# Create Table name for SQL queries
df_movies.registerTempTable("movies")

# Create Table name for SQL queries
df_movie_genres.registerTempTable("movie_genres")

# Create Table name for SQL queries
df_ratings.registerTempTable("ratings")

movies_and_movie_genres = """
    SELECT m.movieId, title, popularity, genre
    FROM movies AS m INNER JOIN movie_genres AS mg ON m.movieId = mg.movieId
"""

joined_all = """
    SELECT m_mg.movieId, title, popularity, genre, userId, rating
    FROM (""" + movies_and_movie_genres + """) AS m_mg
        INNER JOIN ratings AS r
        ON m_mg.movieId = r.movieId
"""
q_joined_all = spark.sql(joined_all)
q_joined_all.registerTempTable("m_mg_r")
# q_joined_all = spark.sql(joined_all)
# q_joined_all.show(10, truncate=False)

all_by_user_genre = """
    SELECT genre, userId, COUNT(*) AS NumRatings, MIN(rating) AS MinRating, MAX(rating) AS MaxRating
    FROM m_mg_r
    GROUP BY userId, genre
"""
q_all_by_user_genre = spark.sql(all_by_user_genre)
q_all_by_user_genre.registerTempTable("m_mg_r_by_user_genre")
# q_all_by_user_genre = spark.sql(all_by_user_genre)
# q_all_by_user_genre.show(truncate=False)


joined_all_on_user_genre = """
    SELECT movieId, title, popularity, rating, m_mg_r.genre, m_mg_r.userId, NumRatings, MinRating, MaxRating
    FROM m_mg_r INNER JOIN m_mg_r_by_user_genre 
        ON (
            m_mg_r.genre = m_mg_r_by_user_genre.genre AND
            m_mg_r.userId = m_mg_r_by_user_genre.userId
            AND (
                m_mg_r.rating = m_mg_r_by_user_genre.MinRating OR
                m_mg_r.rating = m_mg_r_by_user_genre.MaxRating
                )
            )
"""
q_joined_all_on_user_genre = spark.sql(joined_all_on_user_genre)
q_joined_all_on_user_genre.registerTempTable("joined_all_by_user_genre")
# q_joined_all_on_user_genre = spark.sql(joined_all_on_user_genre)
# q_joined_all_on_user_genre.show(truncate=False)


group_by_user_genre_rating = """
    SELECT userId, genre, rating, MAX(popularity) AS MaxPopul
    FROM joined_all_by_user_genre
    GROUP BY userId, genre, rating
"""
q_group_by_user_genre_rating = spark.sql(group_by_user_genre_rating)
q_group_by_user_genre_rating.registerTempTable("group_by_user_genre_rating")
# q_group_by_user_genre_rating = spark.sql(group_by_user_genre_rating)
# q_group_by_user_genre_rating.show(truncate=False)

group_by_user_genre_all = """
    SELECT t1.title, t1.popularity, t1.rating, t1.genre, t1.userId, t1.NumRatings
    FROM joined_all_by_user_genre AS t1
        INNER JOIN group_by_user_genre_rating AS t2
        ON (
            t1.userId = t2.userId AND
            t1.genre = t2.genre AND
            t1.rating = t2.rating AND
            t1.popularity = t2.MaxPopul
            )
"""
q_group_by_user_genre_all = spark.sql(group_by_user_genre_all)
q_group_by_user_genre_all.registerTempTable("group_by_user_genre_all")
# q_group_by_user_genre_all = spark.sql(group_by_user_genre_all)
# q_group_by_user_genre_all.show(truncate=False)

group_by_genre = """
    SELECT genre, MAX(NumRatings) AS MaxNumRatings
    FROM group_by_user_genre_all
    GROUP BY genre
"""
q_group_by_genre = spark.sql(group_by_genre)
q_group_by_genre.registerTempTable("group_by_genre")
# q_group_by_genre = spark.sql(group_by_genre)
# q_group_by_genre.show(truncate=False)

group_by_genre_all = """
    SELECT t1.title, t1.popularity, t1.rating, t1.genre, t1.userId, t1.NumRatings
    FROM group_by_user_genre_all AS t1 INNER JOIN group_by_genre AS t2
        ON (t1.genre = t2.genre AND t1.NumRatings = t2.MaxNumRatings )
"""
q_group_by_genre_all = spark.sql(group_by_genre_all)
q_group_by_genre_all.registerTempTable("group_by_genre_all")
# q_group_by_genre_all = spark.sql(group_by_genre_all)
# q_group_by_genre_all.show(truncate=False)

q5_string = """
    SELECT t1.genre, t1.userId, t1.NumRatings, t2.title AS MaxTitle, t2.rating AS MaxRating, t1.title AS MinTitle, t1.rating AS MinRating
    FROM group_by_genre_all as t1 INNER JOIN group_by_genre_all as t2
        ON (
            t1.genre = t2.genre AND 
            t1.userId = t2.userId AND
            t1.rating < t2.rating
            )
    ORDER BY t1.genre ASC
"""

q5 = spark.sql(q5_string)
q5.show(q5.count(), truncate=False)


# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
