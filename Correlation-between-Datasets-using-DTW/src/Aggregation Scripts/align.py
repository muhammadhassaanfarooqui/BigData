#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
tripdata_lines = lines.mapPartitions(lambda x: reader(x))
date_time = tripdata_lines[2]

output_file = sys.argv[2]

field_count = date_time.map(lambda x: (x[:13])) \
							.map(lambda x: (x, 1) if "2016-02" in x else ("0", 0)) \
							.reduceByKey(lambda x, y: x + y) \
							.sortByKey()

formatted_count = field_count.map(lambda x: ("%s\t%d" % (x[0], x[1])))
formatted_count.saveAsTextFile(output_file)