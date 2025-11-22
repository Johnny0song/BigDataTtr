package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Test07_KeyValuePair {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input/",2)
                        .mapToPair(
                                new PairFunction<String, String, String>() {
                                    @Override
                                    public Tuple2<String, String> call(String s) throws Exception {
                                        String[] split = s.split(" ");
                                        return Tuple2.apply(split[0], split[1]);
                                    }
                                }

                        )
                .collect().forEach(System.out::println);



        jsc.stop();
    }
}
