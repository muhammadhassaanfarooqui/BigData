#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader
import dateutil.parser as dparser

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
tripdata_lines = lines.mapPartitions(lambda x: reader(x))

output_file = sys.argv[2]

injured_idx = [10, 12, 14, 16]
dead_idx = [i+1 for i in injured_idx]

date_column = 0
time_column = 1

def mapper(x):
    try:
        date = str(dparser.parse(x[date_column] + ' ' + x[time_column], fuzzy=True))
        # ls = [(date[:13], (sum([float(x[i]) for i in injured_idx]), sum([float(x[i]) for i in dead_idx])))]
        # ls = (date[:13], (1, 1))
        return (date[:13], (sum([float(x[i]) for i in injured_idx]), sum([float(x[i]) for i in dead_idx])))
    except:
        return ("invalid date", (1, 1))

field_count = tripdata_lines.map(lambda x: mapper(x)) \
                            .map(lambda x: (x[0], (x[1][0], x[1][1]))) \
                            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                            .sortByKey()
#                           .sortBy(lambda x: x[1]) \

formatted_count = field_count.map(lambda x: ("%s\t%.2f, %.2f" % (x[0], x[1][0], x[1][1])))
formatted_count.saveAsTextFile(output_file)
