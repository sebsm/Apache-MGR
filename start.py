
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import explode_outer, size, split, 

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


textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(Column("numWords"))).collect()

wordCounts = textFile.select(explode_outer(split(textFile.value, "\s+")).alias("word")).groupBy("word").count()

wordCounts.collect()