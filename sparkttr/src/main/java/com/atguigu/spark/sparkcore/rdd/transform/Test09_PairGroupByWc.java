package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Test09_PairGroupByWc {
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
                ).groupBy(
                        t -> t._1,2
                ).mapValues(new Function<Iterable<Tuple2<String, Integer>>, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(Iterable<Tuple2<String, Integer>> v1) throws Exception {
                        int sum = 0;
                        String key = null;
                        for (Tuple2<String, Integer> tuple : v1) {
                            key = tuple._1;
                            sum += tuple._2;
                        }
                        return Tuple2.apply(key,sum);
                    }
                })
                .collect().forEach(System.out::println);



        jsc.stop();
    }
}
