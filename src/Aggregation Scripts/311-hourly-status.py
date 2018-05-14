#!/usr/bin/env python

import sys
import dateutil.parser as dateTimeParser
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
tripdata_lines = lines.mapPartitions(lambda x: reader(x))

output_file = sys.argv[2]

date_time_column = 1
status_column = 19

def datetime(column):
	try:
		return str(dateTimeParser.parse(column, fuzzy = True))[:13]
	except:
		return ("00")

field_count = tripdata_lines.map(lambda x: (datetime(x[date_time_column]), x[status_column])) \
							.map(lambda x: ((x[0] , (1.0, 0.0)) if "Close" in x[1] else (x[0], (0.0, 1.0)))) \
							.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
							.sortByKey()

formatted_count = field_count.map(lambda x: ("%s\t%.2f, %.2f" % (x[0], x[1][0], x[1][1])))
formatted_count.saveAsTextFile(output_file)