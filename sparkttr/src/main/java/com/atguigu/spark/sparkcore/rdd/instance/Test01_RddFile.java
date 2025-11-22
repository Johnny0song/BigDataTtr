package com.atguigu.spark.sparkcore.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Test01_RddFile {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark");

//        sparkConf.set("spark.default.parallelism", "1");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        JavaRDD<String> stringJavaRDD = jsc.textFile("sparkttr/data/input//1.txt",3);

        stringJavaRDD.saveAsTextFile("sparkttr/data/output");

        jsc.stop();
    }
}
