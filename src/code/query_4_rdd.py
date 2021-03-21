from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

# Start counting execution time
start_time = time.time()


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def split_year(x):
    return x[3].split("-")[0]


def return_5years_period(year):
    if (year <= 2004):
        return "2000-2004"
    elif (year <= 2009):
        return "2005-2009"
    elif (year <= 2014):
        return "2010-2014"
    elif (year <= 2019):
        return "2015-2019"


spark = SparkSession.builder.appName("query_4_rdd").getOrCreate()

sc = spark.sparkContext

# Emits: (MovieID, (Summary, Year))
rdd_movies = sc.textFile("hdfs://master:9000/movie_data/movies.csv") \
    .map(lambda line: split_complex(line)) \
    .filter(lambda line: (split_year(line) != '')
            and (int(split_year(line)) >= 2000)
            and (int(split_year(line)) <= 2019)) \
    .map(lambda line: (int(line[0]), (line[2], int(split_year(line)))))


# Emits: (MovieID, "Drama")
rdd_movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv") \
    .map(lambda line: split_complex(line)) \
    .filter(lambda line: line[1] == "Drama") \
    .map(lambda line: (int(line[0]), line[1]))

# Emits: (MovieID, ((Summary, Year), "Drama"))
rdd_joined = rdd_movies.join(rdd_movie_genres)

# Emits: ("20XX-20YY", (len(Summary), 1))
summary_by_5years = rdd_joined \
    .map(lambda line: (return_5years_period(line[1][0][1]), (len(line[1][0][0].split(' ')), 1)))

# Result: ("20XX-20YY", Avg Summary)
avg_summary_len_by_5years = summary_by_5years \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda tup: (tup[0], tup[1][0] / tup[1][1])) \
    .sortByKey(ascending=True)

for tup in avg_summary_len_by_5years.collect():
    print(tup[0] + ":", round(tup[1], 2))


# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
