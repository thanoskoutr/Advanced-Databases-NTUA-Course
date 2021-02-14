from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

spark = SparkSession.builder.appName("CSV2Parquet").getOrCreate()

hdfs_path = "hdfs://master:9000/movie_data/"

# Create schema for Table Movies
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

schema_movie_genres = StructType([
    StructField("movieId", IntegerType()),
    StructField("genre", StringType()),
])

schema_ratings = StructType([
    StructField("userId", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", DoubleType()),
    StructField("timestamp", StringType()),
])


# Read CSV to Dataframes
df_movies = spark.read \
    .format("csv") \
    .option("header", "false") \
    .schema(schema_movies) \
    .load(hdfs_path + "movies.csv")

df_movie_genres = spark.read \
    .format("csv") \
    .option("header", "false") \
    .schema(schema_movie_genres) \
    .load(hdfs_path + "movie_genres.csv")

df_ratings = spark.read \
    .format("csv") \
    .option("header", "false") \
    .schema(schema_ratings) \
    .load(hdfs_path + "ratings.csv")

# Save as parquet files
df_movies.write.parquet(hdfs_path + "movies.parquet")
df_movie_genres.write.parquet(hdfs_path + "movie_genres.parquet")
df_ratings.write.parquet(hdfs_path + "ratings.parquet")
