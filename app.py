from pyspark.sql import SparkSession, SparkConf

install_requires=[
        'pyspark=={site.SPARK_VERSION}'
]


def init_spark():
        logFile = "README.md"  # Plik
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

        df = sc.parallelize([1,2,3,4,5])
        df_all = df.collect()
        print("Number of partitions: " + str(df.getNumPartitions()))
        print("Action: First element: " + str(df.first()))
        print(df_all)

        empty_1 = spark.sparkContext.emptyRDD()
        empty_2 = df = spark.sparkContext.parallelize([])

        print(""+str(empty_1.isEmpty()))
        spark.stop()

if __name__ == '__main__':
        main()