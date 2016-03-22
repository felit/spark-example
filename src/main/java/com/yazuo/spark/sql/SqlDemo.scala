package com.yazuo.spark.sql

import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
/**
  */
object SqlDemo {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext("local","jdbc-test")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    var rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("org.postgresql.Driver").newInstance()
        DriverManager.getConnection("jdbc:postgresql://192.168.181.41/trade", "trace", "trace")
      },
      "select merchant_name from trade.merchant where merchant_id >=? and merchant_id <=?", 2, 100, 3,
      r => r.getString(1))
    val resultRdd = rdd.filter(_.contains("雅"))

    println(rdd.filter(_.contains("雅")).count())
    sc.stop()
  }
}
