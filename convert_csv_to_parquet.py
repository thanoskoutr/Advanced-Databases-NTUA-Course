from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV2Parquet").getOrCreate()

hdfs_path = "hdfs://master:9000/movie_data/"

# Read CSV to Dataframes
movies = spark.read.format("csv").option("header", "false").load(hdfs_path + "movies.csv")
movie_genres = spark.read.format("csv").option("header", "false").load(hdfs_path + "movie_genres.csv")
ratings = spark.read.format("csv").option("header", "false").load(hdfs_path + "ratings.csv")

# Show spark dataframes
#df.show(n=2)

# Save as parquet files
movies.write.parquet(hdfs_path + "movies.parquet")
movie_genres.write.parquet(hdfs_path + "movie_genres.parquet")
ratings.write.parquet(hdfs_path + "ratings.parquet")