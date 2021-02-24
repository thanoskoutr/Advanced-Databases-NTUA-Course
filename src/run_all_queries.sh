#!/bin/bash

# Log Directory
log_dir="logs"

# Delete executions times txt
rm -f "queries_exec_times.txt"

# Run all queries in Spark and save logs
for python_file in query_*.py; do
    spark-submit "query_${query}_${type}.py" > "${log_dir}/log_${query}_${type}.txt" 2>&1
done

