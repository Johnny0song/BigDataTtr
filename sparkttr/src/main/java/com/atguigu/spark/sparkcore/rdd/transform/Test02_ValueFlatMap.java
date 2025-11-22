package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

public class Test02_ValueFlatMap {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input//1.txt",1)
                        .flatMap(new FlatMapFunction<String, String>() {
                            @Override
                            public Iterator<String> call(String s) throws Exception {
                                String[] split = s.split(" ");


                                return Arrays.asList(split).iterator();
                            }
                        }).collect().forEach(System.out::println);



        jsc.stop();
    }
}
