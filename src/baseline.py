#!/usr/bin/python

import sys
import os
import numpy as np
import pandas as pd
import string
import time

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
from pyspark.ml.fpm import FPGrowth

import utility as ut
import cleaner

def main():
	if len(sys.argv) != 3:	
		print("\nError found:",ut.red("incorrect arguments."),"\nTerminating.\n\nRun using this command:\n\n\t",ut.grn("python3 cleaner.py <csv_file> <debug>\n"))
		sys.exit(1)

	file_name = sys.argv[1]
	debug = int(sys.argv[2])

	if debug:
		print(ut.cyn("\nStarting spark context (local)"))

	sc = pyspark.SparkContext('local[*]')
	sc.setLogLevel("ERROR")
	
	if debug:
		print(ut.cyn("\nSpark executor threads count:"),ut.cyn(sc.defaultParallelism),"\nStarting execution timer.")
	
	start_time = time.time()

	check, dataf = cleaner.load_data(file_name, debug)	

	if not check:
		sys.exit(2)

	if debug:
		print(ut.cyn("\nRunning baseline algorithm"))

	dataf = dataf.rdd.map(lambda x:(x[0], x[1].split(" "),x[2])).toDF(["index","tokens","date"])

	fpGrowth = FPGrowth(itemsCol="tokens", minSupport=0.006, minConfidence=0.005)
	model = fpGrowth.fit(dataf)

	model = model.freqItemsets.filter(size(col("items")) > 1)
	model = model.sort(col("freq").desc())
	
	if debug > 1:	
		model.show(200, False)

	model.toPandas().to_csv('./data/output/baseline.csv')

	if debug:
		print("Terminating, execution time:",(time.time() - start_time), "\n")

#MAIN
if __name__ == '__main__':
	main()