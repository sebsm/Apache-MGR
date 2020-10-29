
from __future__ import print_function

from pyspark import SparkContext, SparkConf

from operator import add

conf = SparkConf().setMaster("local[1]").setAppName("Word count")
context = SparkContext(conf=conf)

file = context.textFile("hdfs///README.txt")

data = file.flatMap(lambda line: line.split(' ')) \
           .map(lambda x: (x,1)) \
           .reduceByKey(add)

output = data.collect()

for (word, count) in output:
    print("%s: %i" % (word, count))

data.saveAsTextFile("hdfs///output.txt")