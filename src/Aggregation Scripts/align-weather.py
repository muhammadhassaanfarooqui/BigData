#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
weatherData_lines = lines.mapPartitions(lambda x: reader(x))

date_column = 0
time_column = 1
temperature_column = 4

output_file = sys.argv[2]

def datetime(date, time):
	try:
		return ("%s-%s-%s %s" % (date[:4], date[4:6], date[6:], time[:2]))
	except:
		return ("00")

def connvertToFLoat(datetime, temp):
	try:
		return (datetime, (float(temp), 1.00))
	except ValueError:
		return ("00", (0.00, 1.00))

field_count = weatherData_lines.map(lambda x: (datetime(x[date_column], x[time_column]), x[temperature_column] )) \
							.map(lambda x: (connvertToFLoat(x[0], x[1])) if ("999" not in x[1]) else ("0", (0.00,1.00))) \
							.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1])) \
							.sortByKey()

formatted_count = field_count.map(lambda x: ("%s\t%.2f" % (x[0], x[1][0]/x[1][1])))
formatted_count.saveAsTextFile(output_file)
