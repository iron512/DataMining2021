#!/usr/bin/python

import os, psutil

class bcolors:
	GREEN = '\033[92m'
	YELLOW = '\033[93m'
	CYAN = '\033[96m'
	RED = '\033[91m'
	RESET = '\033[0m'

def grn(text):
	return bcolors.GREEN + str(text) + bcolors.RESET

def ylw(text):
	return bcolors.YELLOW + str(text) + bcolors.RESET

def cyn(text):
	return bcolors.CYAN + str(text) + bcolors.RESET

def red(text):
	return bcolors.RED + str(text) + bcolors.RESET

def memory():
	process = psutil.Process(os.getpid())
	return "{:.2f}".format(process.memory_info().rss / 1000000) + " MB"
