package com.yazuo.spark.sql

import com.yazuo.spark.Common
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext


/**
  */
object HiveSql extends App {
  val sc = new SparkContext("local", "hive-sql-context")
  val hiveCtx = new HiveContext(sc)
  val result = hiveCtx.read.json(s"${Common.PATH_PREFIX}/src/main/resources/demo.json")
  result.foreach(str => {
    println(str)
  })
  sc.stop()
}
