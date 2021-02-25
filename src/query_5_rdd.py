from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
import sys

# Start counting execution time
start_time = time.time()


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def map_all_by_user(tup):
    user_dict = {}
    userId = tup[1][1][0]
    # movieId = tup[0]
    title = tup[1][0][0]
    popularity = tup[1][0][1]
    genres = tup[1][0][2]
    rating = tup[1][1][1]
    # user_dict[genre] = (NumRatings, MinPopul, MaxPopul, MinTitle, MaxTitle, MinRating, MaxRating)
    for genre in genres:
        user_dict[genre] = (1, popularity, popularity,
                            title, title, rating, rating)
    return (userId, user_dict)


def reduce_all_by_user(user1_dict, user2_dict):
    for genre, value1 in user1_dict.items():
        if genre in user2_dict:
            value2 = user2_dict[genre]
            NumRatings = value1[0] + value2[0]
            if value1[5] < value2[5] or \
                    (value1[5] == value2[5] and value1[1] > value2[1]):
                MinPopul = value1[1]
                MinTitle = value1[3]
                MinRating = value1[5]
            else:
                MinPopul = value2[1]
                MinTitle = value2[3]
                MinRating = value2[5]
            if value1[6] > value2[6] or \
                    (value1[6] == value2[6] and value1[2] > value2[2]):
                MaxPopul = value1[2]
                MaxTitle = value1[4]
                MaxRating = value1[6]
            else:
                MaxPopul = value2[2]
                MaxTitle = value2[4]
                MaxRating = value2[6]
            user1_dict[genre] = (NumRatings, MinPopul, MaxPopul,
                                 MinTitle, MaxTitle, MinRating, MaxRating)
            del user2_dict[genre]
    merged_dict = {**user1_dict, **user2_dict}
    return merged_dict


def flatMap_all_genres(tup):
    userId = tup[0]
    user_dict = tup[1]
    return ((genre, (userId, values)) for genre, values in user_dict.items())


spark = SparkSession.builder.appName("query_5_rdd").getOrCreate()

sc = spark.sparkContext


# Emits: (MovieID, (Title, Popularity))
rdd_movies = sc.textFile("hdfs://master:9000/movie_data/movies.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), (line[1], line[7])))

# Emits: (MovieID, [Genres])
rdd_movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), line[1])) \
    .groupByKey() \
    .mapValues(list)  # Only for pyspark


# Emits: (UserID, [(MovieID, Rating)])
# rdd_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
#     .map(lambda line: split_complex(line)) \
#     .map(lambda line: (int(line[0]), (int(line[1]), float(line[2])))) \
#     .groupByKey() \
#     .mapValues(list)  # Only for pyspark

# Emits: (MovieID, (UserID, Rating))
rdd_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[1]), (int(line[0]), float(line[2]))))

# Emits: (MovieID, (Title, Popularity, [Genres]))
rdd_movies_and_movie_genres = rdd_movies.join(rdd_movie_genres) \
    .map(lambda tup: (tup[0], (tup[1][0][0], tup[1][0][1], tup[1][1])))


# Map: (MovieID, ((Title, Popularity, [Genres]), (UserID, Rating)))
# Emits: (UserID, {Genre: (NumRatings, MinPopul, MaxPopul, MinTitle, MaxTitle, MinRating, MaxRating)})
rdd_all_by_user = rdd_movies_and_movie_genres.join(rdd_ratings) \
    .map(map_all_by_user) \
    .reduceByKey(reduce_all_by_user)

# FlatMap: (Genre, (UserID, (NumRatings, MinPopul, MaxPopul, MinTitle, MaxTitle, MinRating, MaxRating)))
# Reduce: (UniqGenre, (UserID, (NumRatings, MinPopul, MaxPopul, MinTitle, MaxTitle, MinRating, MaxRating)))
# Emits: (UniqGenre, UserID, NumRatings, MaxTitle, MaxRating, MinTitle, MinRating)
rdd_all_by_genre = rdd_all_by_user \
    .flatMap(flatMap_all_genres) \
    .reduceByKey(lambda x, y: x if x[1][0] > y[1][0] else y) \
    .sortByKey() \
    .map(lambda tup: (tup[0], tup[1][0], tup[1][1][0], tup[1][1][4], tup[1][1][6], tup[1][1][3], tup[1][1][5]))
