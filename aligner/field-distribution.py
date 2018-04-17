#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
lines = sc.textFile(sys.argv[1])
lines = lines.mapPartitions(lambda x: reader(x))

column = int(sys.argv[2])
output_file = sys.argv[3]

field_count = lines.map(lambda x: (x[column][:5], 1)).reduceByKey(lambda x, y: x + y).sortByKey()
formatted_count = field_count.map(lambda x: ("%s\t%d" % (x[0], x[1])))
formatted_count.saveAsTextFile(output_file)
