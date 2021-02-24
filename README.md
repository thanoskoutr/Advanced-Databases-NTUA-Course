# Advanced-Databases-NTUA-Course

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

### Transfer src files to Okeanos VM
From outside of `/src` on Local:
```bash
rsync -rv src user@83.212.79.226:/home/user/Project
```

### Transfer log files from Okeanos VM
From `/src` on Local:
```bash
rsync -rv user@83.212.79.226:/home/user/Project/src/{logs,queries_exec_times.txt} .
```

### Submit queries to Spark
```bash
spark-submit file.py
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
```
hadoop fs -mkdir hdfs://master:9000/movie_data
```

Transfer the 3 `.csv` files to new directory:
```
hadoop fs -put movie_genres.csv movies.csv ratings.csv hdfs://master:9000/movie_data
```

List directory to see if files are transfered:
```
hadoop fs -ls hdfs://master:9000/movie_data
```

### Task 2
Run `convert_csv_to_parquet.py` with Spark:
```
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
- [ ] Query 5:
  - [ ] RDD
  - [ ] SQL with CSV (infer schema)
  - [ ] SQL with Parquet

- [ ] Combine SQL implementations:
  - CSV
  - Parquet

### Task 4
Run `run_all_queries.sh` in order run all queries and get logs and execution times:
```bash
./run_all_queries.sh
```
Run `plot_queries_exec_times.py` in order to create the bar plot for each query's execution time:
```bash
./plot_queries_exec_times.py queries_exec_times.txt
```

## Part 2

### Task 1

### Task 2

### Task 3

### Task 4