package com.yazuo.spark.sql

import com.yazuo.spark.Common
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  */
class HiveSql {

}

object HiveSql extends App {
  val sc = new SparkContext("local", "hive-sql-demo")
  val sqlCtx = new SQLContext(sc)
  val path = s"${Common.PATH_PREFIX}/src/main/resources/demo.json"
  val table = sqlCtx.read.json(path)
  table.registerTempTable("demo")
  val resultDataFrame = sqlCtx.sql("select * from demo")
  resultDataFrame.show()
}