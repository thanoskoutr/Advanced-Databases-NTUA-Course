from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType

spark = SparkSession.builder.appName("create_movie_genres_100").getOrCreate()

hdfs_path = "hdfs://master:9000/movie_data/"

# Create schema for Table movie_genres
# schema_movie_genres = StructType([
#     StructField("movieId", IntegerType()),
#     StructField("genre", StringType()),
# ])

# Read CSV to Dataframes
df_movie_genres = spark.read \
    .format("csv") \
    .option("header", "false") \
    .load(hdfs_path + "movie_genres.csv")

# Take first 100 lines of dataframe
df_movie_genres = df_movie_genres.limit(100)

# Save as csv files
df_movie_genres.write.format("csv").save(hdfs_path + "movie_genres_100.csv")

# # Read CSV to Dataframes (for parquet)
# df_movie_genres = spark.read \
#     .format("csv") \
#     .option("header", "false") \
#     .schema(schema_movie_genres) \
#     .load(hdfs_path + "movie_genres.csv")

# # Save as parquet files
# df_movie_genres.write.parquet(hdfs_path + "movie_genres_100.parquet")
