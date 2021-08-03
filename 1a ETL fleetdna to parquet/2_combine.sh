#!/bin/bash

# File: 2_combine.sh
# Project: clustering-analysis-domain-agnostic-features-2018
# Authors: Alexander van Roijen, Caleb Phillips
# License: BSD 3-Clause
# Copyright (c) 2021 Alliance for Sustainable Energy LLC

echo "This script will combine all csvs in /object/fleetdna/vehicle_data and put it in the hdfs fleetdna directory in one csv called result.csv"
for file in $(ls /object/fleetdna/vehicle_data/v_*.csv); #this works though
do
  tail -n +2 $file|hdfs dfs -appendToFile - hdfs:///fleetdna/result2.csv
  echo $file
done
