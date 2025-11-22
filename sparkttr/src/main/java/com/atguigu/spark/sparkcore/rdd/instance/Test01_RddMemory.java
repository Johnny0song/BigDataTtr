package com.atguigu.spark.sparkcore.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Test01_RddMemory {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("spark");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = jsc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"));

        stringJavaRDD.foreach(s -> System.out.println(s));
        jsc.stop();
    }
}
