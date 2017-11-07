package com.felit.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

public class StreamingDemo {
    public static void main(String args[]) {
        String master = "local[1]";

        SparkConf conf = new SparkConf().setMaster(master).setAppName("应用名称:NetworkWordCount");
        JavaStreamingContext jssc = null;
        JavaPairDStream<String, Integer> rst = null;

        try {
            //流计算功能的入口点
            jssc = new JavaStreamingContext(conf, Durations.seconds(3));

            //流的入口
            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

            //计算过程
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
            JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
            wordCounts.print();
            if (rst == null) {
                rst = wordCounts;
            } else {
                rst.join(wordCounts);
            }
            rst.print();
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (jssc != null) {
                jssc.close();
            }
        }
    }
}
