# Spark

## Run python code in Spark
In order to run python code in Spark e.g. `wordcount.py`, run:
```
cd example_codes/
spark-submit wordcount.py
```

## Save Spark logs and output to file
In order to save the logs and output that Spark produces during execution, run the 
following command that redirects the output and errors into the `log.txt` file:
```
spark-submit wordcount.py > log.txt 2>&1
``` 
Now the logs and the output are in the `log.txt` file

## Save Spark logs and output to different files
In order to save the logs and output that Spark produces during execution, 
in different files, run the following command that 
- Redirects the output into `out.txt` and 
- Redirects the logs into `log.txt
```
spark-submit wordcount.py >out.txt 2>log.txt
```
Now the logs are in `log.txt` 
and the output are in`out.txt`.

## Run python code with PySpark
Instead you can run python code in the PySpark interpreter.

Start Pyspark:
```
pyspark
```