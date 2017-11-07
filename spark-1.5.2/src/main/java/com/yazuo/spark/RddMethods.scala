package com.yazuo.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * def
 * flatMap[U](f: (T) ⇒ TraversableOnce[U])(implicit arg0: ClassTag[U]): RDD[U]
 * Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
 *
 * Internally, each RDD is characterized by five main properties:

 * A list of partitions
 * A function for computing each split
 * A list of dependencies on other RDDs
 * Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file)
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for reading data from a new storage system) by overriding these functions. Please refer to the Spark paper for more details on RDD internals.
 */
object RddMethods {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-spark-rdd-methods")
    val sc = new SparkContext(conf)
    val textFile: RDD[String] = sc.textFile("/data/source/sparkDemo/src/main/java/com/yazuo/spark/RddMethods.scala")
//    val counts = textFile.flatMap(line => {
////      println(line)
//      line.split(" ")
//    })
    val demoRdd:RDD[String] = sc.textFile("/data/source/sparkDemo/src/main/java/com/yazuo/spark/LocalDemo.scala")
    val unitRdd: RDD[String] = textFile ++ demoRdd
    unitRdd.foreach(str=>{
      println(str)
    })
    println("demoRDD:" + demoRdd.count())
    println("textFile:" + textFile.count())
    println("unitRdd:" + unitRdd.count())
    println(unitRdd.name)
    println(unitRdd.dependencies)
//    println(counts)
//    println(counts.distinct())
  }
}
