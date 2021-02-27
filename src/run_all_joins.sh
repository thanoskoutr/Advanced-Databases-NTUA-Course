#!/bin/bash

# Log Directory
log_dir="output"
code_dir="code"

if [ ! -d "${log_dir}" ]; then
    mkdir ${log_dir}
fi

# Delete executions times txt
rm -f "joins_exec_times.txt"

# Run all joins in Spark and save exec times
for python_file in ${code_dir}/join_*.py; do
    file_name=`echo $python_file | awk '{split($0,a,"/"); print a[2]}'`
    echo "Executing: spark-submit ${python_file} > ${log_dir}/log_${file_name}.txt 2>&1"
    spark-submit ${python_file} > "${log_dir}/log_${file_name}.txt" 2>&1
done
