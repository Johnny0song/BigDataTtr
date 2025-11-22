package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Test06_ValueSortBy {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input/",1)
                        .sortBy(
                                new Function<String, Integer>() {
                                    @Override
                                    public Integer call(String s) throws Exception {
                                        return s.length();
                                    }
                                },
                                false,
                                2
                        ).collect().forEach(System.out::println);



        jsc.stop();
    }
}
