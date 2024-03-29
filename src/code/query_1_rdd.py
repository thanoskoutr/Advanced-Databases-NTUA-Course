from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

# Start counting execution time
start_time = time.time()


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def calc_profit(x):
    cost = int(x[5])
    revenue = int(x[6])
    profit = ((revenue-cost)/cost)*100
    return profit


def split_year(x):
    return x[3].split("-")[0]


spark = SparkSession.builder.appName("query_1_rdd").getOrCreate()

sc = spark.sparkContext

# GroupByYear: Result (Year, (Profit, Title))
rdd = sc.textFile("hdfs://master:9000/movie_data/movies.csv") \
        .map(lambda line: split_complex(line)) \
        .filter(lambda line: (split_year(line) != '')
                and (line[5] != '')
                and (line[6] != '')
                and (int(line[5]) != 0)
                and (int(line[6]) != 0)
                and (int(split_year(line)) >= 2000)) \
        .map(lambda line: (split_year(line), (calc_profit(line), line[1])))

# Result (Year, Movie)
most_profit_by_year = rdd.reduceByKey(lambda x, y: x if x[0] > y[0] else y) \
                         .sortByKey() \
                         .map(lambda result: (result[0], result[1][1]))


# Print Movies with the most profit by year
for i in most_profit_by_year.collect():
    print("Year:", i[0], "Title:", i[1])

# Calculate and Print Execution time
total_time = time.time() - start_time

with open('queries_exec_times.txt', 'a+') as fp:
    fp.write(sys.argv[0].split('/')[-1] + ': ' +
             str(total_time) + ' seconds\n')

print("--- %s seconds ---" % (time.time() - start_time))
