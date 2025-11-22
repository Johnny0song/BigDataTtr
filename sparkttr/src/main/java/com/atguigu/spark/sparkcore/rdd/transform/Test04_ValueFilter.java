package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Test04_ValueFilter {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input//1.txt",1)
                        .filter(
                                new Function<String, Boolean>() {
                                    @Override
                                    public Boolean call(String v1) throws Exception {
                                        return v1.length() > 11;
                                    }
                                }
                        ).collect().forEach(System.out::println);



        jsc.stop();
    }
}
