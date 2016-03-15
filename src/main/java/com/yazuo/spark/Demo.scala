package com.yazuo.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

/**
 *
 */
class Demo {

}

object Demo extends App {
  val sc = new SparkContext("spark://storm3:7077", "wordcount")

  val logRdd: RDD[String] = sc.textFile("hdfs://192.168.181.21:9000/log-analysis/ecrm1.log/20151225.log", 1)

  val regex = new Regex( """.*\"handler\":\"(\d*)\".*""")
  /*val handlerRdd = logRdd.map(new Function[String, String] {
    def apply(s: String) = {
      var regex(handler) = s
      Console.print(handler)
      handler
    }
  })*/
  val handlerRdd = logRdd.map(line => """\"handler\":\"(\d*)\"""".r.findAllIn(line).mkString)

  Console.println(handlerRdd.count())
  Console.println(logRdd.count())

  sc.textFile("hdfs://192.168.181.21:9000/log-analysis/ecrm1.log/20151226.log", 1).map(line => """\"handler\":\"(\d*)\"""".r.findAllIn(line).mkString).distinct().collect()
}