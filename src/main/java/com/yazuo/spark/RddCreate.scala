package com.yazuo.spark

import org.apache.spark.SparkContext

/**
 * It is important to note that each time we call a new action, the entire RDD must be
computed “from scratch.” To avoid this inefficiency, users can persist intermediate
results, as we will cover in “Persistence (Caching)”
  */
object RddCreate extends App {
  val sc: SparkContext = new SparkContext("local", "rdd-create")
  val rdd = sc.parallelize(List("hello", "world","hello test"))
  println(rdd)
  val hellordd = rdd.filter(str => str.contains("hello"))
  hellordd.foreach(println)
  sc.stop()
}
