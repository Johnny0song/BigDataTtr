package com.atguigu.spark.sparkcore.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Test07_SaveAsObjectFile {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> stringStringJavaPairRDD = jsc.textFile("sparkttr/data/input/", 2)
                .flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {

                        String[] split = s.split(" ");
                        return Arrays.stream(split)
                                .map(word -> Tuple2.apply(word, 1))
                                .iterator();
                    }
                });

        stringStringJavaPairRDD.saveAsObjectFile("sparkttr/data/output2");



        jsc.stop();
    }
}
