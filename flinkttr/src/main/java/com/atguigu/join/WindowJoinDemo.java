package com.atguigu.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowJoinDemo {
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


        DataStream<String> apply = ds1Wm.join(ds2Wm)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {


                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {


                        return first + " <==> " + second;
                    }
                });

        apply.print();


        env.execute();


    }
}
