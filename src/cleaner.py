#!/usr/bin/python

import sys
import os
import numpy as np
import pandas as pd
import string
import re
import time

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size

import utility as ut

def load_data(path, debug = 1):
	spark = SparkSession.builder.appName("DataMiningProject").getOrCreate()

	if not os.path.exists(path):
		print("\nError found:", ut.red("invalid file."), "\nTerminating.\n\nArgument passed ("+ ut.ylw(file_csv) +") is not a valid file.\n")
		return False, None

	if debug:
		print(ut.cyn("\nCollecting entries"))

	dataf = spark.read.format("csv").option("header", "true").load(path)
	
	if debug:
		print("Done!")

	return True, dataf

def extract(dataf, debug = 1):
	if debug:
		print(ut.cyn("\nExtracting entries"))
		
	if debug:
		print("Total entries:",ut.red(dataf.count()))
	
	dataf = dataf.select("text","date")
	dataf = dataf.filter(dataf.text.isNotNull())
	dataf = dataf.filter(dataf.date.isNotNull())
	
	if debug:
		print("Not-null text and date entries:",ut.ylw(dataf.count()))
	
	#Split date in set of numbers and perform operations on it
	dataf = dataf.rdd.map(lambda x:(x[0],x[1].split(" ")[0].split("-"))).toDF(["text","date"])
	dataf = dataf.filter(size(col("date")) == 3)

	if debug:
		print("Valid date entries:",ut.ylw(dataf.count()))

	dataf = dataf.filter(dataf.date[0] == 2020)
	dataf = dataf.filter(dataf.date[1] >= 7)
	dataf = dataf.filter(dataf.date[1] <= 8)
	
	if debug:
		print("Correct and valid date entries:",ut.grn(dataf.count()))

	return dataf

def clean(dataf, debug = 1):
	stop_words = set(stopwords.words('english'))

	if debug:
		print(ut.cyn("\nCleansing entries"))

	rdd = dataf.rdd
	#remove slash(/), dots(.), hashes(#), ats(@)  and the source (https://...)
	rdd = rdd.map(lambda x:([w for w in x[0].lower().replace("covid19", "covid").replace("/","").replace("t.co","tco").replace(".", " ").replace("@", " ").replace("#", " ").split(" ") if True or not w.startswith("https")],x[1]))
	#remove the stopwords and remove the time from the timestamps (i.e. keep the data)
	rdd = rdd.map(lambda x:([w for w in x[0] if (not w.lower() in stop_words) and w.isalnum()],x[1]))
	rdd = rdd.map(lambda x:(list(set(x[0])),x[1]))
	rdd = rdd.map(lambda x:(' '.join(x[0]),'-'.join(x[1])))

	dataf = rdd.toDF(["tokens","date"])
	dataf = dataf.filter(dataf.tokens != "")
	return dataf

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
	

	if debug:
		print(ut.cyn("\nUpdating nl toolkit data"))

	nltk.download('stopwords')
	nltk.download('punkt')

	check, dataf = load_data(file_name, debug)	

	if not check:
		sys.exit(2)

	dataf = extract(dataf, debug)
	dataf = clean(dataf, debug)
	
	if debug > 1:
		dataf.show(30, False)

	if  debug:
		print(ut.cyn("\nSaving to \"data/clean_dataset.csv\""))		
	
	dataf.toPandas().to_csv('./data/clean_dataset.csv')

	if debug:
		print("Terminating, execution time:",(time.time() - start_time), "\n")

#MAIN
if __name__ == '__main__':
	main()