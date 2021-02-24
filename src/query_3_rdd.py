from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def map_all_genres(tup):
    rating = tup[1][0]
    genres = tup[1][1]
    return ((genre, (rating, 1)) for genre in genres)


spark = SparkSession.builder.appName("query_3").getOrCreate()

sc = spark.sparkContext

# Start counting execution time
start_time = time.time()

# Emits: (MovieID, Rating)
rdd_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[1]), float(line[2])))

# Emits: (MovieID, [Genres])
rdd_movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), line[1])) \
    .groupByKey()
# .mapValues(list) ## Only for pyspark

# Emits: (MovieID, (Rating, [Genres]))
rdd_joined = rdd_ratings.join(rdd_movie_genres)

# Emits: (MovieID, (Avg Rating, [Genres]))
genres_avg_rating_by_movieID = rdd_joined \
    .map(lambda tup: (tup[0], (tup[1][0], tup[1][1], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1], x[2] + y[2])) \
    .map(lambda tup: (tup[0], (tup[1][0]/tup[1][2], tup[1][1])))

# Emits: (Genre, Avg Rating, Count)
avg_ratings_per_genre = genres_avg_rating_by_movieID \
    .flatMap(map_all_genres) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda tup: (tup[0], tup[1][0]/tup[1][1], tup[1][1]))

for i in avg_ratings_per_genre.collect():
    print(i)


# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
