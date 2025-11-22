package com.atguigu.spark.sparkcore.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Test03_First {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = jsc.textFile("sparkttr/data/input/", 2);

        System.out.println(stringJavaRDD.first());



        jsc.stop();
    }
}
