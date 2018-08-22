import os
from pprint import pprint
import sys
import json
import math
# import statistics

def average(a):
	return sum(a) / len(a)

def avg(a):
	if len(a) == 0:
		return 999999
	max_a = max(a)
	a.remove(max_a)
	min_a = min(a)
	a.remove(min_a)
	return sum(a) / len(a)

def parse_exp4a(eval_result):
	
	metrics = {}
	metrics["recovery"] = {}
	metrics["thpt"] = {}

	for config in ["tar", "tard", "dis", "tardis"]:
		metrics["recovery"][config] = {}
		metrics["thpt"][config] = {}
		for alpha in ["1", "10", "100"]:
			metrics["recovery"][config][alpha] = {}
			metrics["thpt"][config][alpha] = {}
			for ar in ["1", "10", "100"]:
				metrics["recovery"][config][alpha][ar] = -1
				metrics["thpt"][config][alpha][ar] = -1

	for d in os.listdir(eval_result):
		if "exp" not in d:
			continue
		
		config=d.split('-')[3]
		ar=d.split('-')[10]
		alpha=d.split('-')[12]

		# print config, ar, alpha

		thpt_text = open(eval_result + '/' + d + '/clienth1-throughput.out').readlines()
		thpt = []
		for i in range(1, len(thpt_text)):
			thpt.append(int(thpt_text[i].replace('\n', '')))

		data = json.load(open(eval_result + '/' + d + '/client-h1-metrics.json'))
		start = 240
		if "recover-duration" in data:
			dur = int(data["recover-duration"])
			dur= dur / 1000
			metrics["recovery"][config][alpha][ar]= data["recover-duration"]
			# for i in range(0, len(thpt)):
			# 	if thpt[i-1] * 0.8 > thpt[i]:
			# 		start = i
			# 		break
			# print thpt
			# print "start", start
			metrics["thpt"][config][alpha][ar] = avg(thpt[start:start+dur])

		print config, ar, alpha, start, metrics["recovery"][config][alpha][ar], metrics["thpt"][config][alpha][ar]

	for config in ["tar", "tard", "dis", "tardis"]:
		row = ",1,10,100\n"
		for alpha in ["1", "10", "100"]:
			row += alpha
			row += ","
			for ar in ["1", "10", "100"]:
				row+=str(metrics["recovery"][config][alpha][ar])
				row+=","
			row+="\n"
		print config
		print row

	for config in ["tar", "tard", "dis", "tardis"]:
		row = ",1,10,100\n"
		for alpha in ["1", "10", "100"]:
			row += alpha
			row += ","
			for ar in ["1", "10", "100"]:
				row+=str(metrics["thpt"][config][alpha][ar])
				row+=","
			row+="\n"
		print config
		print row


def median(lst):
	if len(lst) == 0:
		return -1
	sortedLst = sorted(lst)
	lstLen = len(lst)
	index = (lstLen - 1) / 2
	return sortedLst[index]

def mean(data):
    """Return the sample arithmetic mean of data."""
    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')
    return sum(data)/float(n) # in Python 2 use sum(data)/float(n)

def _ss(data):
    """Return sum of square deviations of sequence data."""
    c = mean(data)
    ss = sum((x-c)**2 for x in data)
    return ss

def stddev(data, ddof=0):
    """Calculates the population standard deviation
    by default; specify ddof=1 to compute the sample
    standard deviation."""
    n = len(data)
    if n < 2:
    	return 0
        # raise ValueError('variance requires at least two data points')
    ss = _ss(data)
    pvar = ss/(n-ddof)
    return pvar**0.5
	# return 0

parse_exp4a("/Users/haoyuh/Documents/PhdUSC/EventualPersistence/proj/BG/haoyu/results-tardis")

