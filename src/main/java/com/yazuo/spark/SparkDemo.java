package com.yazuo.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.actors.threadpool.Arrays;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class SparkDemo {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "test-java-client");
//        JavaSparkContext sc = new JavaSparkContext("spark://192.168.181.23:7077", "test-java-client");
        JavaRDD<String> textFile = sc.textFile("/home/congsl/apps/storm/dockerfile-repository/nginx/Dockerfile");
        Map<String, Integer> result = textFile.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterable<Object> call(String s) throws Exception {
                System.out.println(s);
                return Arrays.asList(s.split(" "));
            }
        }).map(new Function<Object, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> call(Object v1) throws Exception {
                System.out.println(v1);
                Map<String, Integer> map = new HashMap<String, Integer>();
                map.put(v1.toString(), 1);
                return map;
            }
        }).reduce(new Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> call(Map<String, Integer> v1, Map<String, Integer> v2) throws Exception {
                System.out.println("v1:" + v1);
                System.out.println("v2:" + v2);
                for (String key : v2.keySet()) {
                    if (v1.get(key) == null) {
                        v1.put(key, v2.get(key));
                    } else {
                        v1.put(key, v1.get(key) + v2.get(key));
                    }
                }
                return v1;
            }
        });

        System.out.println(result);
        System.out.println(textFile.count());
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        DataFrame df = sqlContext.jdbc("jdbc:mysql://localhost:3306/activiti?user=root&password=admin", "ACT_ID_INFO");
        df.show();
//        JavaSQLContext sqlContext = new JavaSQLContext(sparkContext);
    }
}
