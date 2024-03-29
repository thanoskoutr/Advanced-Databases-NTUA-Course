# Advanced-Databases-NTUA-Course
A project for the Advanced Databases course at ECE NTUA, using Apache Spark to extract info from movie rating's big dataset.

We are using the [MovieLens Dataset](https://grouplens.org/datasets/movielens/latest), containing over 27,000,000 ratings for 58,000 movies by 280,000 users. The dataset is saved on Apache HDFS in both CSV and Parquet format.

We are using the Apache Spark framework with Python, utilizing both the older RDD API and the newer Dataframe API. We are also using Spark SQL, allowing us to run directly SQL queries to the distributed database.

The main task of the project is to run queries on the dataset using the RDD API and Spark SQL and compare their execution times. The other task is to implement the Broadcast and Repartition Joins in the RDD API and compare their performance.

[Report of the project in Greek](Report.md)

## Database Schema
`movies.csv` fields:
```
col[0] = movieId
col[1] = title
col[2] = summary
col[3] = timestamp
col[4] = duration
col[5] = cost
col[6] = revenue
col[7] = popularity
```

`movie_genres.csv` fields:
```
col[0] = movieId
col[1] = genre
```

`movies_ratings.csv` fields:
```
col[0] = userId
col[1] = movieId
col[2] = rating
col[3] = timestamp
```

## After Reboot
### Change hostnames
On Master:
```bash
sudo hostname master
```
On Slave:
```bash
sudo hostname slave
```

### Start HDFS
```bash
start-dfs.sh
```

### Start Spark
```bash
start-all.sh
```

### Check status
```bash
jps && ssh slave jps
```

## Transfer files to/from VM
### Transfer src files to Okeanos VM
From outside of `/src` on Local:
```bash
rsync -rv src user@83.212.79.226:/home/user/Project
```

### Transfer log files from Okeanos VM
From `/src` on Local:
```bash
rsync -rv user@83.212.79.226:/home/user/Project/src/{output,queries_exec_times.txt} .
```
```bash
rsync -rv user@83.212.79.226:/home/user/Project/src/{output,joins_exec_times.txt} .
```
```bash
rsync -rv user@83.212.79.226:/home/user/Project/src/{output,optimizer_exec_times.txt} .
```


## Submit queries to Spark
```bash
spark-submit file.py
```

## Convert markdown to pdf
### Install needed tools
```bash
pip3 install grip
sudo apt install wkhtmltopdf
```

### Convert `.md` to .`pdf`
```bash
grip Report.md --export Report.html && wkhtmltopdf Report.html Report.pdf
```

## Part 1

### Task 1
Download `movie_data`:
```bash
wget www.cslab.ntua.gr/courses/atds/movie_data.tar.gz
```

Untar the 3 `.csv` files:
```bash
tar -xvzf movie_data.tar.gz
```

Create a `movie_data` directory on hdfs:
```bash
hadoop fs -mkdir hdfs://master:9000/movie_data
```

Transfer the 3 `.csv` files to new directory:
```bash
hadoop fs -put movie_genres.csv movies.csv ratings.csv hdfs://master:9000/movie_data
```

List directory to see if files are transfered:
```bash
hadoop fs -ls hdfs://master:9000/movie_data
```

### Task 2
Run `convert_csv_to_parquet.py` with Spark:
```bash
spark-submit convert_csv_to_parquet.py > log_convert_csv_to_parquet.txt 2>&1
```
Converts the 3 `.csv` to `.parquet` files and saves log file.


### Task 3
#### To-Do
- [x] Query 1:
  - [x] RDD
  - [x] SQL with CSV (infer schema)
  - [x] SQL with Parquet
- [x] Query 2:
  - [x] RDD
  - [x] SQL with CSV (infer schema)
  - [x] SQL with Parquet
- [x] Query 3:
  - [x] RDD
  - [x] SQL with CSV (infer schema)
  - [x] SQL with Parquet
- [x] Query 4:
  - [x] RDD
  - [x] SQL with CSV (infer schema)
  - [x] SQL with Parquet
- [x] Query 5:
  - [x] RDD
  - [x] SQL with CSV (infer schema)
  - [x] SQL with Parquet

### Task 4
#### Run all queries
Run `run_all_queries.sh` in order run all queries and get logs and execution times:
```bash
./run_all_queries.sh
```
**Execution times in:** `queries_exec_times.txt`

#### Create bar plot
Run `plot_queries_exec_times.py` in order to create the bar plot for each query's execution time:
```bash
./plot_queries_exec_times.py queries_exec_times.txt
```

## Part 2

### Task 1
Implementation of broadcast join in `code/join_broadcast.py`
### Task 2
Implementation of repartition join in `code/join_repartition.py`
### Task 3
#### Create `movie_genres_100.csv`
Run `create_movie_genres_100.py` on VM, where the `movie_genres.csv` is:
```bash
./create_movie_genres_100_local.py movie_genres.csv
```
Takes the first 100 lines of `movie_genres.csv` and saves them to `movie_genres_100.csv` and saves it to hdfs.
#### Count number of lines
```bash
hadoop fs -cat hdfs://master:9000/movie_data/movie_genres_100.csv | wc -l
```
#### Run all joins
Run `run_all_joins.sh` in order run the 2 joins and get logs and execution times:
```bash
./run_all_joins.sh
```
**Execution times in:** `joins_exec_times.txt`

### Task 4
#### Run all queries
Run `run_all_optimizer.sh` in order run 2 joins between `movie_genres` and `ratings` with and without the query optimizer:
```bash
./run_all_optimizer.sh
```
**Execution times in:** `optimizer_exec_times.txt`

#### Create bar plot
Run `plot_optimizer_exec_times.py` in order to create the bar plot for each query's execution time:
```bash
./plot_optimizer_exec_times.py optimizer_exec_times.txt
```