#!/bin/bash

# Log Directory
log_dir="output"
code_dir="code"

if [ ! -d "${log_dir}" ]; then
    mkdir ${log_dir}
fi

# Delete executions times txt
rm -f "optimizer_exec_times.txt"

# Run all scripts in Spark and save exec times
echo "Executing: spark-submit ${code_dir}/optimizer_joins.py Y > ${log_dir}/log_optimizer_joins.py_Y.txt 2>&1"
spark-submit ${code_dir}/optimizer_joins.py Y > ${log_dir}/log_optimizer_joins.py_Y.txt 2>&1

echo "Executing: spark-submit ${code_dir}/optimizer_joins.py N > ${log_dir}/log_optimizer_joins.py_N.txt 2>&1"
spark-submit ${code_dir}/optimizer_joins.py N > ${log_dir}/log_optimizer_joins.py_N.txt 2>&1
