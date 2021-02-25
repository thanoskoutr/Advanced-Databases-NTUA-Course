#!/usr/bin/python3
import subprocess
import sys
import csv

if len(sys.argv) < 2:
    print('Usage: create_movie_genres_100.py <movie_genres.csv>')
    exit(-1)

movie_genres_csv = sys.argv[1]
path = movie_genres_csv.split('/')[:-1]
name = movie_genres_csv.split('/')[-1].split('.')[0]+'_100.csv'
movie_genres_100_csv = '/'.join(path+[name])

line_count = 0
movie_genres_list = []

# Read 100 lines from csv
with open(movie_genres_csv) as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    for row in csv_reader:
        if line_count < 100:
            movie_genres_list.append(row)
            line_count += 1

# Write 100 lines to CSV
csv_file = open(movie_genres_100_csv, "w")
writer = csv.writer(csv_file)

for row in movie_genres_list:
    writer.writerow(row)

csv_file.close()


# Save new csv to hadoop
command = 'hadoop fs -put %s hdfs://master:9000/movie_data' % \
    (movie_genres_100_csv)
subprocess.run(command, shell=True)
