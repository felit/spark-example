package com.felit.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ScalaStreamingDemo {
  def main(args: Array[String]): Unit = {
    // the main entry points for all streaming functionally, two execution threads
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    // a batch interval of 1 second
    val ssc = new StreamingContext(conf, Seconds(1))
    // 输入流DStream,a DStream that represents streaming data
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
