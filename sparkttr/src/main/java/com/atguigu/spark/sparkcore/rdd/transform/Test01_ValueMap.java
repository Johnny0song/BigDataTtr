package com.atguigu.spark.sparkcore.rdd.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;

public class Test01_ValueMap {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.textFile("sparkttr/data/input//1.txt",1)
                        .map(new Function<String, String>() {
                            @Override
                            public String call(String v1) throws Exception {
                                String[] split = v1.split(" ");
                                StringBuffer stringBuffer = new StringBuffer();
                                for (int i = 0; i < split.length; i++) {
                                    stringBuffer.append(split[i]).append("_map ");
                                }
                                return stringBuffer.toString();
                            }
                        }).collect().forEach(System.out::println);


        jsc.stop();
    }
}
