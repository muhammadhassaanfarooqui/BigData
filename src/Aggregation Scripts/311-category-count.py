#!/usr/bin/env python

import sys
import dateutil.parser as dateTimeParser
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
tripdata_lines = lines.mapPartitions(lambda x: reader(x))

input_borough = (sys.argv[2]).lower()
output_file = sys.argv[3]

date_time_column = 1
complaint_column_number = 5
borough_column = 25


def datetime(column):
	try:
		return str(dateTimeParser.parse(column, fuzzy = True))[:13]
	except:
		return ("00")

def categoryCount(datetime, category, borough):
	category = category.lower()
	borough = borough.lower()

	if input_borough in borough:
		if "noise" in category:
			return (datetime, (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
		elif "heat" in category or "hot water" in category or "water leak" in category or "water system" in category or "plumbing" in category:
			return (datetime, (0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
		elif "traffic signal" in category or "street sign" in category or "sidewalk condition" in category or "curb condition" in category:
			return (datetime, (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
		elif "sanitiation" in category or "missed collection" in category or "overflowing" in category:
			return (datetime, (0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0))
		elif "derelict vehicle" in category:
			return (datetime, (0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0))
		elif "snow" in category or "standing water" in category:
			return (datetime, (0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0))
		elif "animal" in category:
			return (datetime, (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0))
		elif "lead" in category:
			return (datetime, (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0))
		elif "school" in category:
			return (datetime, (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0))
		else:
			return (datetime, (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
	else:
		return ("00", (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))

field_count = tripdata_lines.map(lambda x: categoryCount(datetime(x[date_time_column]), x[complaint_column_number], x[borough_column])) \
							.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5], x[6] + y[6], x[7] + y[7], x[8] + y[8])) \
							.sortByKey()

formatted_count = field_count.map(lambda x: ("%s\t%.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f" % (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6], x[1][7], x[1][8])))
formatted_count.saveAsTextFile(output_file)