from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

# Start counting execution time
start_time = time.time()


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("query_2_rdd").getOrCreate()

sc = spark.sparkContext

# Emits: (User, Rating)
rdd = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
        .map(lambda line: split_complex(line)) \
        .map(lambda line: (int(line[0]), (float(line[2]), 1)))

# (Unique User, Avg Rating)
avg_rating_per_user = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda tup: (tup[0], tup[1][0]/tup[1][1]))

# (Unique User, Avg Rating > 3)
over3_rating_per_user = avg_rating_per_user.filter(lambda tup: tup[1] > 3.0)

all_ratings = avg_rating_per_user.count()
over3_ratings = over3_rating_per_user.count()

result_percent = round((over3_ratings/all_ratings)*100, 2)

print(str(result_percent) + "%")

# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
