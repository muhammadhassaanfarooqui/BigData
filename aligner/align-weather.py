#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
weatherData_lines = lines.mapPartitions(lambda x: reader(x))
date = weatherData_lines[0]
temperature = weatherData_lines[4]

output_file = sys.argv[2]

field_count = weatherData_lines.map(lambda x: (date, temperature )) \
							.map(lambda x: (x[0], (float(x[1]), 1.00)) if ("999" not in x[1]) and ("201101" in x[0]) else ("0", (0.00,1.00))) \
							.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1])) \
							.sortByKey()
#							.sortBy(lambda x: x[1]) \

formatted_count = field_count.map(lambda x: ("%s\t%.2f" % (x[0], x[1][0]/x[1][1])))
formatted_count.saveAsTextFile(output_file)
