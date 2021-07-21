#!/usr/bin/python

import sys
import os
import numpy as np
import pandas as pd
import string
import time
import datetime

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, LongType
from pyspark.sql.functions import col, size, array_contains
from pyspark.ml.fpm import FPGrowth

import utility as ut
import cleaner
  
def main():
	if len(sys.argv) != 4:	
		print("\nError found:",ut.red("incorrect arguments."),"\nTerminating.\n\nRun using this command:\n\n\t",ut.grn("python3 cleaner.py <csv_file> <debug> <store_graph_data>\n"))
		sys.exit(1)

	file_name = sys.argv[1]
	debug = int(sys.argv[2])
	graph = int(sys.argv[3])

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
		print(ut.cyn("\nRunning the custom solution"))

	dataf = dataf.rdd.map(lambda x:(x[0], x[1].split(" "),x[2])).toDF(["index","tokens","date"])
	if debug > 1:
		dataf.show(30, False)

	start_str = dataf.agg({'date': 'min'}).collect()[0][0].split("-")
	end_str = dataf.agg({'date': 'max'}).collect()[0][0].split("-")

	start_date = datetime.datetime(int(start_str[0]), int(start_str[1]), int(start_str[2]))
	end_date = datetime.datetime(int(end_str[0]), int(end_str[1]), int(end_str[2]))

	spark = SparkSession.builder.appName('DataMiningProject').getOrCreate()
	merged = spark.createDataFrame([], StructType([
		StructField('items', ArrayType(StringType()), True),
  		StructField('freq', LongType(), True)]))

	wearmask_array = []	
	spreadhelp_array = []	
	deaths24h_array = []

	coronacovid_array = []
	reportedcases_array = []

	while start_date <= end_date-datetime.timedelta(days=2):
		print(start_date, start_date+datetime.timedelta(days=2))
		test = dataf.filter(dataf.date >= start_date)
		test = test.filter(test.date <= start_date+datetime.timedelta(days=3))

		fpGrowth = FPGrowth(itemsCol="tokens", minSupport=0.004, minConfidence=0.005)
		model = fpGrowth.fit(test)

		model = model.freqItemsets.filter(size(col("items")) > 1)
		#model = model.sort(col("freq").asc())

		if graph:
			wearmask = model.filter(array_contains(col("items"),"wear"))
			wearmask = wearmask.filter(array_contains(col("items"),"mask"))

			spreadhelp = model.filter(array_contains(col("items"),"spread"))
			spreadhelp = spreadhelp.filter(array_contains(col("items"),"help"))
			spreadhelp = spreadhelp.filter(array_contains(col("items"),"covid"))
			spreadhelp = spreadhelp.filter(size(col("items")) == 3)

			deaths24h = model.filter(array_contains(col("items"),"24"))
			deaths24h = deaths24h.filter(array_contains(col("items"),"deaths"))
			deaths24h = deaths24h.filter(array_contains(col("items"),"hours"))
			deaths24h = deaths24h.filter(size(col("items")) == 3)

			coronacovid = model.filter(array_contains(col("items"),"coronavirus"))
			coronacovid = coronacovid.filter(array_contains(col("items"),"covid"))
			coronacovid = coronacovid.filter(size(col("items")) == 2)
			
			reportedcases = model.filter(array_contains(col("items"),"new"))
			reportedcases = reportedcases.filter(array_contains(col("items"),"cases"))
			reportedcases = reportedcases.filter(size(col("items")) == 2)

			if not len(wearmask.head(1)) == 0:
				wearmask_array.append(wearmask.collect()[0][1])
			else:
				wearmask_array.append(0)

			if not len(spreadhelp.head(1)) == 0:
				spreadhelp_array.append(spreadhelp.collect()[0][1])
			else:
				spreadhelp_array.append(0)

			if not len(deaths24h.head(1)) == 0:
				deaths24h_array.append(deaths24h.collect()[0][1])
			else:
				deaths24h_array.append(0)

			if not len(coronacovid.head(1)) == 0:
				coronacovid_array.append(coronacovid.collect()[0][1])
			else:
				coronacovid_array.append(0)

			if not len(reportedcases.head(1)) == 0:
				reportedcases_array.append(reportedcases.collect()[0][1])
			else:
				reportedcases_array.append(0)

		merged = merged.union(model)

		start_date += datetime.timedelta(days=1)

	if graph:
		graph_output = open("./data/output/sampling.csv", "w")
		graph_output.write("wear_mask," + ", ".join([str(x) for x in wearmask_array])+"\n")
		graph_output.write("spread_help," + ", ".join([str(x) for x in spreadhelp_array])+"\n")
		graph_output.write("deaths_24h," + ", ".join([str(x) for x in deaths24h_array])+"\n")
		graph_output.write("corona_covid," + ", ".join([str(x) for x in coronacovid_array])+"\n")
		graph_output.write("reported_cases," + ", ".join([str(x) for x in reportedcases_array])+"\n")
		graph_output.close()

	model = merged.groupBy('items').count()
	model = model.filter(col("count") > 7)
	model = model.filter(col("count") < 14)

	model = model.sort(col("count").desc())

	model.toPandas().to_csv('./data/output/results.csv')

	if debug:
		print("Terminating, execution time:",(time.time() - start_time), "\n")

#MAIN
if __name__ == '__main__':
	main()