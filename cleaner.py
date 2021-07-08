#!/usr/bin/python

import csv
import sys
import os
import numpy as np
import pandas as pd

import utility as ut

def load_data(file_csv):
	#check if the argument passed exist as file
	if not os.path.exists(file_csv):
		print("\nError found:", ut.red("invalid file."), "\nTerminating.\n\nArgument passed ("+ ut.ylw(file_csv) +") is not a valid file.\n")
		return False, None

	data = pd.read_csv(file_csv)
	return True, data

def main():
	if len(sys.argv) != 3:	
		print("\nError found:",ut.red("incorrect arguments."),"\nTerminating.\n\nRun using this command:\n\n\t",ut.grn("python3 cleaner.py <csv_file> <debug>\n"))
		sys.exit(1)

	file_name = sys.argv[1]
	debug = int(sys.argv[2])

	check, data = load_data(file_name)	
	if not check:
		sys.exit(2)


#MAIN
if __name__ == '__main__':
	main()
