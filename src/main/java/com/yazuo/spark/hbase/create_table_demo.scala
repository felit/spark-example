package com.yazuo.spark.hbase

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.HBaseAdmin

/**
  */
object create_table_demo extends App {

  var conf = HBaseConfiguration.create()
  val admin = new HBaseAdmin(conf)
  val descriptor = new HTableDescriptor("trans_water")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("hbase.master", "localhost")

  descriptor.addFamily(new HColumnDescriptor("dimension"))
  descriptor.addFamily(new HColumnDescriptor("measure"))
  admin.createTable(descriptor)

}
