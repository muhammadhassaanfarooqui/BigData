#!/usr/bin/env python

import sys
import dateutil.parser as dateTimeParser
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
tripdata_lines = lines.map(lambda x: x.replace('\0', '')).mapPartitions(lambda x: reader(x))

output_file = sys.argv[2]

date_time_column = 1
trip_distance_column = 4
fare_amount_column = -7
tip_column = -4

def datetime(column):
	try:
		return str(dateTimeParser.parse(column, fuzzy = True))[:13]
	except:
		return "00"

def convertAndClean(a, b, c, d, e):
	try:
		distance = float(b)
		if distance < 100.00:
			return (a, (distance, float(c), float(d), e))
		else:
			return ("00", (0.0, 0.0, 0.0, 1.0))
	except ValueError:
		return ("00", (0.0, 0.0, 0.0, 1.0))

def mapper(row):
	if len(row) == 0:
		return []
	else:
		dateTime = datetime(row[date_time_column])
		if dateTime[:6] is "2016-1": 
			return [(dateTime , row[trip_distance_column], row[fare_amount_column-2], row[tip_column-2] )]
		else:
			return [(dateTime , row[trip_distance_column], row[fare_amount_column], row[tip_column] )]


field_count = tripdata_lines.flatMap(lambda x: mapper(x)) \
							.map(lambda x: (convertAndClean(x[0], x[1], x[2], x[3], 1.00))) \
							.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3])) \
							.sortByKey()

formatted_count = field_count.map(lambda x: ("%s\t%.2f, %.2f, %.2f, %.2f" % (x[0], x[1][0]/x[1][3], x[1][1]/x[1][3], x[1][2]/x[1][3], x[1][3])))
formatted_count.saveAsTextFile(output_file)