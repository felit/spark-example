package com.yazuo.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 */
public class SparkDemo {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local","test-java-client");
        JavaRDD<String> textFile = sc.textFile("/home/congsl/apps/storm/dockerfile-repository/nginx/Dockerfile");
        System.out.println(textFile.count());
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        DataFrame df = sqlContext.jdbc("jdbc:mysql://localhost:3306/activiti?user=root&password=admin", "ACT_ID_INFO");
        df.show();
//        JavaSQLContext sqlContext = new JavaSQLContext(sparkContext);
    }
}
