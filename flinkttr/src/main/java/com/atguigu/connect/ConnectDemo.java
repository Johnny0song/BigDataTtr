package com.atguigu.connect;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 8888);




        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1Wm = ds1.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(Tuple2.of(split[0], Integer.parseInt(split[1])));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                        return element.f1*1000L;
                    }
                })).setParallelism(1);

        ds1Wm.print();



        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2Wm = ds2.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(Tuple3.of(split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2])));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofMillis(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                        return element.f1*1000L;
                    }
                })).setParallelism(1);

        ds2Wm.print();


/*
        ds1Wm.connect(ds2Wm).map(new CoMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
            @Override
            public String map1(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }

            @Override
            public String map2(Tuple3<String, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        }).print();
*/

/*        ds1Wm.connect(ds2Wm).keyBy(value -> value.f0, value -> value.f0)
                        .map(new CoMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public String map1(Tuple2<String, Integer> value) throws Exception {
                                return value.f0;
                            }

                            @Override
                            public String map2(Tuple3<String, Integer, Integer> value) throws Exception {
                                return value.f0;
                            }
                        }).print();*/

/*        ds1Wm.keyBy(value -> value.f0)
                        .connect(ds2Wm.keyBy(value -> value.f0))
                                .map(new CoMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                                    @Override
                                    public String map1(Tuple2<String, Integer> value) throws Exception {
                                        return value.f0;
                                    }

                                    @Override
                                    public String map2(Tuple3<String, Integer, Integer> value) throws Exception {
                                        return value.f0;
                                    }
                                }).print();*/

        ds1Wm.connect(ds2Wm)
                        .keyBy(value -> value.f0, value -> value.f0)
                                .process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {


                                    @Override
                                    public void processElement1(Tuple2<String, Integer> value, KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                                        out.collect(value.f0   +"来自连接1");
                                    }

                                    @Override
                                    public void processElement2(Tuple3<String, Integer, Integer> value, KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {

                                        out.collect(value.f0 + "来自连接2");
                                    }
                                }).print();




        env.execute();
    }
}
