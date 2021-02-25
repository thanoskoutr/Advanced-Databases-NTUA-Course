from pyspark.sql import SparkSession
from io import StringIO
import csv


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def repartition_join(tup):
    return tup


spark = SparkSession.builder.appName("repartition_join_rdd").getOrCreate()

sc = spark.sparkContext

# Test Join on two csv files
# Emits: (MovieID, (UserID, Rating, Timestamp))
rdd_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[1]), (int(line[0]), float(line[2]), line[3])))

# Emits: (MovieID, Genre)
rdd_movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), line[1]))

# Emits: (MovieID, ((UserID, Rating, Timestamp), Genre))
rdd_joined = rdd_ratings.join(rdd_movie_genres)

# Print some records
rdd_joined.take(10)
