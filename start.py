
import pyspark
from pyspark.sql import SparkSession


spark = SparkSession \
.builder \
.appName("Python Spark test") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

textFile = spark.read.text("README.md")

textFile.count()

textFile.first()

linesWithSpark = textFile.filter(textFile.value.contains("Spark"))

textFile.filter(textFile.value.contains("Spark")).count()


textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(column("numWords"))).collect()

wordCounts = textFile.select(explode_outer(split(textFile.value, "\s+")).alias("word")).groupBy("word").count()

wordCounts.collect()