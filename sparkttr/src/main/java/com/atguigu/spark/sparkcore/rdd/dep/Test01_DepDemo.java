package com.atguigu.spark.sparkcore.rdd.dep;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Test01_DepDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = jsc.textFile("sparkttr/data/input", 2);
        System.out.println("=================lineage==================");
        System.out.println(stringJavaRDD.toDebugString());

        JavaRDD<String> stringJavaRDD1 = stringJavaRDD
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {

                        return Arrays.asList(s.split(" ")).iterator();
                    }
                });
        System.out.println("=================lineage==================");
        System.out.println(stringJavaRDD1.toDebugString());

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD1
                .mapToPair(
                        new PairFunction<String, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(String s) throws Exception {
                                return Tuple2.apply(s, 1);
                            }
                        }
                );
        System.out.println("=================lineage==================");
        System.out.println(stringIntegerJavaPairRDD.toDebugString());


        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD
                .reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                return v1 + v2;
                            }
                        }, 2
                );
        System.out.println("=================lineage==================");
        System.out.println(stringIntegerJavaPairRDD1.toDebugString());

        stringIntegerJavaPairRDD1.saveAsTextFile("sparkttr/data/output1");


        jsc.stop();
    }
}
