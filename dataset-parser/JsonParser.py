from pyspark.sql import SparkSession
#from pyspark import SparkContext
import sys

spark = SparkSession\
		.builder \
		.appName("Json Parser") \
		.getOrCreate()

#sc = SparkContext()

fname = sys.argv[1]

def parse(fname):
	dataFrame = spark.read.option("multiline", "true").json(fname)
	dataFrame.createOrReplaceTempView("dataSet")
	data = spark.sql("select data from dataSet")
	dataRDD = data.rdd
	df1 = spark.sparkContext.parallelize(dataRDD.collect()[0][0]).toDF()
	df1.show()

parse(fname)


