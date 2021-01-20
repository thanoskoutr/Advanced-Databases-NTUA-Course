# Hadoop Guide

## Hadoop (HDFS) Commands
Every command is HDFS is like a UNIX command, but with the suffix:
```
hadoop fs -UNIX_COMMAND
```

### Create a directory in HDFS
In order to create an `examples` directory to HDFS, run:
```
hadoop fs -mkdir hdfs://master:9000/examples
```

### List directory contents in HDFS
In order to execute the `ls` command in the home folder on HDFS, run:
```
hadoop fs -ls hdfs://master:9000/.
```

### Tranfer files to HDFS
In order to tranfer a file e.g. `departments.csv` in our current directory
to HDFS in the `examples` directory, run:
```
hadoop fs -put departments.csv hdfs://master:9000/examples
```

### Tranfer files from HDFS
In order to tranfer a file e.g. `departments.csv` from the `examples` directory in HDFS
in our current directory, run:
```
hadoop fs -get hdfs://master:9000/examples/departments.csv .
```

## Visit Hadoop Web UI
Go with your browser to your Masters public IP address and append the port `50070`:
```
http://83.212.79.226:50070
```