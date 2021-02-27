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


# Input: tup1 = ([(Value, tag1)], [(Value, tag2)]), tup2 = ([(Value, tag1)], [(Value, tag2)])
def repartition_join_reduce(tup1, tup2):
    tag1 = tup1[0][0][1]
    tag2 = tup2[0][0][1]
    if tag1 == tag2:
        list1 = tup1[0] + tup2[0]
        list2 = tup1[1] + tup2[1]
    elif tag1 < tag2:
        list1 = tup1[0] + tup2[1]
        list2 = tup1[1] + tup2[0]
    else:
        list1 = tup1[1] + tup2[0]
        list2 = tup1[0] + tup2[1]
    return (list1, list2)


# Input: (Key, (list1, list2))
def repartition_join_flatMap(tup):
    key = tup[0]
    list1 = tup[1][0]
    list2 = tup[1][1]
    if len(list2) == 0:
        return []
    return ((key, (i[0], j[0])) for i, j in product(list1, list2))


spark = SparkSession.builder.appName("repartition_join_rdd").getOrCreate()

sc = spark.sparkContext

# Test Join on two csv files
# Emits: (MovieID, ([((UserID, Rating, Timestamp), tag)], []))
rdd_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[1]), ([((int(line[0]), float(line[2]), line[3]), 'r')], [])))
# Data mapped for repartition join

# Emits: (MovieID, ([(Genre, tag)], []))
rdd_movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres_100.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), ([(line[1], 'mg')], [])))
# Data mapped for repartition join

# Compine the two datasets to be joined
rdd_union = rdd_ratings.union(rdd_movie_genres)

rdd_join = rdd_union \
    .reduceByKey(repartition_join_reduce) \
    .flatMap(repartition_join_flatMap)

# Get joined table
rdd_join.collect()

# Calculate and Print Execution time
total_time = time.time() - start_time

with open('joins_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
