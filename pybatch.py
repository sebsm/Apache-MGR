from pyspark.sql.functions import *

textFile = spark.read.text("README.md")

textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(col("numWords"))).collect()[Row(max(numWords)=15)]