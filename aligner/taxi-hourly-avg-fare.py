#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
tripdata_lines = lines.mapPartitions(lambda x: reader(x))

output_file = sys.argv[2]

date_time_column = 2
fare_amount_column = -7

field_count = tripdata_lines.map(lambda x: (x[date_time_column][:13], x[fare_amount_column])) \
							.map(lambda x: (x[0], (float(x[1]), 1.00)) if ("999" not in x[1]) and ("2016-01" in x[0]) else ("0", (0.00,1.00))) \
							.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1])) \
							.sortByKey()
#							.sortBy(lambda x: x[1]) \

formatted_count = field_count.map(lambda x: ("%s\t%.2f" % (x[0], x[1][0]/x[1][1])))
formatted_count.saveAsTextFile(output_file)