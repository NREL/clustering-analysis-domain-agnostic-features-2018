#!/usr/bin/env python

# File: 1_condense.sh
# Project: clustering-analysis-domain-agnostic-features-2018
# Authors: Alexander van Roijen, Caleb Phillips
# License: BSD 3-Clause
# Copyright (c) 2021 Alliance for Sustainable Energy LLC

from mpi4py import MPI
import sys
import argparse
import os
import pandas as pd
import json
import numpy as np
import datetime
import time
import csv 

# Parse command line args
desc = 'Combine FleetDNA Data' 
parser = argparse.ArgumentParser(description=desc)
parser.add_argument('--path',help='Path to FleetDNA data')
args = parser.parse_args()

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

variable_whitelist=["ts","lat","lon","speed","grade"]

print "I am %d in a world of %d" % (rank,size)
    
if rank == 0:
  todo = [f for f in os.listdir(args.path) if os.path.isdir(os.path.join(args.path, f)) and not os.path.isfile(os.path.join(args.path,f+".csv")) ]
  if len(todo) == 0:
    print "Nothing to do"
    comm.Abort()
    quit()
  num = float(len(todo))/size
  chunks = [todo[i:i + int(num)] for i in range(0, (size -1)*int(num), int(num))]
  chunks.append(todo[(size-1)*int(num):])
  todo = chunks
else:
  todo = None

todo = comm.scatter(todo,root=0)
print "I am %d and have %d items to do" % (rank,len(todo)) 

def parseDateTime(x):
  try:
    return x
    #return datetime.datetime.strptime(x,"%Y-%m-%dZ%H:%M:%S")
  except:
    return None

def parseFloat(x):
  try:
    return float(x)
  except:
    return None

def process_json(fh,var):
  if var == "ts":
    return json.load(fh)
  else:
    return map(parseFloat,json.load(fh))

def date_dir_to_df(dtdir):
  df = {}
  for var in variable_whitelist:
    jfile = var + ".json"
    if not os.path.isfile(os.path.join(dtdir,jfile)):
      df[var] = None
    else:
      with open(os.path.join(dtdir,jfile),"r") as fh:
        df[var] = process_json(fh,var)
  if len(df.keys()) == 0 or 'ts' not in df.keys():
    return {}
  return df

for vdir in todo:
  with open(os.path.join(args.path,vdir+".csv"), 'wb') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['vdir','dtdir'] + variable_whitelist)
    for dtdir in os.listdir(os.path.join(args.path,vdir)):
      print "%s -> %s" % (vdir,dtdir)
      df = date_dir_to_df(os.path.join(args.path,vdir,dtdir))
      if df['ts'] is None:
        continue
      for i in range(0,len(df['ts'])):
        csvwriter.writerow([vdir,dtdir] + map(lambda x: str(df[x][i]) if not df[x] is None else None,variable_whitelist))
