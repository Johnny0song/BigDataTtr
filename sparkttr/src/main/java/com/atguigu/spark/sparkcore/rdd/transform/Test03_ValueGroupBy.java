package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class Test03_ValueGroupBy {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input//1.txt",1)
                        .groupBy(new Function<String, Integer>() {
                            @Override
                            public Integer call(String v1) throws Exception {
                                return v1.length() % 2;
                            }
                        }).collect().forEach(System.out::println);



        jsc.stop();
    }
}
