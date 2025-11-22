package com.atguigu.spark.sparkcore.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Test01_Collect {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input/",2)
                .collect().forEach(System.out::println);



        jsc.stop();
    }
}
