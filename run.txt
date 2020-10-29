@ECHO OFF
START spark-class org.apache.spark.deploy.master.Master
START spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.2:7077
START spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.2:7077
START spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.2:7077
START spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.2:7077
