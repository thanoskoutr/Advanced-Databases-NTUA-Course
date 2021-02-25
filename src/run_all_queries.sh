#!/bin/bash

# Log Directory
log_dir="logs"

if [ ! -d "${log_dir}" ]; then
    mkdir ${log_dir}
fi

# Delete executions times txt
rm -f "queries_exec_times.txt"

# Run all queries in Spark and save logs
for python_file in query_*.py; do
    echo "Executing: spark-submit ${python_file} > ${log_dir}/log_${python_file}.txt 2>&1"
    spark-submit ${python_file} > "${log_dir}/log_${python_file}.txt" 2>&1
done

