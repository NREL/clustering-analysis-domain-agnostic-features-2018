# Fleet DNA Big Data ETL

This directory contains ETL (extract-transform-load) code, which prepares a canonical parquet format which the SDK uses from legacy FleetDNA data formats.

  * 1\_condense.py - A scalable MPI script to combine files in the vehicle\_data JSON format (e.g., vehicle\_data/v\_42/2012-03-02/\*.json) into simplified CSVs (e.g., v\_42.csv)
  * 2\_combine.sh - A single threaded script that combines all vehicle CSVs into a single CSV on the HDFS (Hadoop Filesystem)
  * 3\_parqueify.py - A pyspark script to prepare a parquet version, with any preprocessing necessary which is the backing data format for the SDK

## 1 (Condense)

The condense script creates a per-vehicle CSV file in a standardized form, with a downsampled set of fields, using the hierarchy of JSON files as input.

Here's an example of how to run the condense step in parallel on a single node using 16 simulataneous I/O threads:

```
mpirun --host localhost -np 16 python /full/path/to/condense.py --path /object/fleetdna/vehicle_data
```

## 2 (Combine)

This bash script expects to read all csvs from a designated diretory and combine them all.
Currenty this script expects 1 argument, argument being the path to where all the csvs are located.
Then it combines them all using hdfs -appendToFile and writes to hdfs://fleetdna/result.csv

```
bash combine.sh /path/to/csvs/
```


## 3 (Parqueify)

Expects two arguments,
First: The location of the CSV file to be parqueted
Second: The destination location of the parquet

```
path/to/sparkplug-examples/spark-submit.sh cluster <queue> /full/path/to/parqueify.py /path/to/largeFile.csv /path/to/largeParquet
```
