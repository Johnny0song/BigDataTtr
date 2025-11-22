package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Test11_PairSortByKey {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input/",2)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {

                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }).mapToPair(
                        new PairFunction<String, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(String s) throws Exception {
                                return Tuple2.apply(s, 1);
                            }
                        }
                ).reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                return v1 + v2;
                            }
                        },2
                ).sortByKey(
                        false
                )
                .collect().forEach(System.out::println);



        jsc.stop();
    }
}
