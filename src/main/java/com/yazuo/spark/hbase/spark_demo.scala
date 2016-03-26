package com.yazuo.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object spark_demo {
  val conf = HBaseConfiguration.create()
  //  var hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
  //    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
  //    classOf[org.apache.hadoop.hbase.client.Result])
//  val rdd: RDD = null


  var sparkConfig = new SparkConf().setAppName("Connector")
  var sc: SparkContext = new SparkContext(sparkConfig)


}
