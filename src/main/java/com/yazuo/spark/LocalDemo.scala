package com.yazuo.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
local 本地单线程
local[K] 本地多线程（指定K个内核）
local[*] 本地多线程（指定所有可用内核）
spark://HOST:PORT 连接到指定的 Spark standalone cluster master，需要指定端口。
mesos://HOST:PORT 连接到指定的 Mesos 集群，需要指定端口。
yarn-client客户端模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
yarn-cluster集群模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
  */
object LocalDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
//    conf.setMaster("spark://localhost:7077")
//    conf.setMaster("spark://127.0.1.1:7077")
    conf.setAppName("local-spark-demo")
    val sc = new SparkContext(conf)
    val textFile:RDD[String] = sc.textFile("/data/source/sparkDemo/src/main/java/com/yazuo/spark/LocalDemo.scala")
    val counts = textFile.flatMap(line => {
      println(line)
      line.split(" ")
    }).map((_, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("/data/source/sparkDemo/target/result")
    sc.stop()
  }
}
