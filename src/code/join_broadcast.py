from pyspark.sql import SparkSession
from io import StringIO
import csv
from itertools import product
import time
import sys

# Start counting execution time
start_time = time.time()


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("broadcast_join_rdd").getOrCreate()

sc = spark.sparkContext


# Test Join on two csv files
# Emits: (MovieID, Genre)
rdd_movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres_100.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), line[1])) \
    .groupByKey() \
    .mapValues(list)  # Only for pyspark

# Collect small rdd in a dictionary -> MovieID:[Genres]
dict_movie_genres = rdd_movie_genres.collectAsMap()

# Broadcast small rdd
dict_movie_genres_broad = sc.broadcast(dict_movie_genres)


# Input: (Key, Value)
def broadcast_join_flatMap(tup):
    key = tup[0]
    value = tup[1]
    if key in dict_movie_genres_broad.value:
        return ((key, (genre, value)) for genre in dict_movie_genres_broad.value[key])
    else:
        return []


# Emits: (MovieID, (UserID, Rating, Timestamp))
rdd_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[1]), (int(line[0]), float(line[2]), line[3])))

rdd_join = rdd_ratings.flatMap(broadcast_join_flatMap)

# Get joined table
rdd_join.collect()

# Calculate and Print Execution time
total_time = time.time() - start_time

with open('joins_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
