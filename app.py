from pyspark.sql import SparkSession

install_requires=[
        'pyspark=={site.SPARK_VERSION}'
]


def init_spark():
        logFile = "README.md"  # Should be some file on your system
        spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
        sc = spark.sparkContext
        logData = spark.read.text(logFile).cache()
        return spark, sc, logData

def main():
        spark, sc, logData = init_spark()
        numAs = logData.filter(logData.value.contains('a')).count()
        numBs = logData.filter(logData.value.contains('b')).count()
        numCs = logData.filter(logData.value.contains('c')).count()
        print("Lines with a: %i, lines with b: %i, lines with c: %i" % (numAs, numBs, numCs))

        spark.stop()