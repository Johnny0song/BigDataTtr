package com.atguigu.process;

import com.atguigu.pojos.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop101", 9999);


        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDs = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] values = value.trim().split(" ");
                WaterSensor waterSensor = new WaterSensor(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
                return waterSensor;
            }
        });

        SingleOutputStreamOperator<WaterSensor> wsDsWithWk = wsDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs()*1000L;
                    }
                })
                .withIdleness(Duration.ofMillis(2000))
        );

        wsDsWithWk.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private long lastWatermark = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                        String currentKey = ctx.getCurrentKey();
                        long currentWatermark = ctx.timerService().currentWatermark();



                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        Long timestamp = ctx.timestamp();
                        out.collect("当前key："+currentKey
                                +", 当前处理时间："+currentProcessingTime
                                +", 元素时间戳："+timestamp
                                +", 当前水位线："+currentWatermark);

                        ctx.timerService().registerEventTimeTimer(5000L);


                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        String currentKey = ctx.getCurrentKey();
                        long currentWatermark = ctx.timerService().currentWatermark();

                        System.out.println("定时器触发！当前key："+currentKey
                                +", 触发时间："+timestamp
                                +", 当前水位线："+currentWatermark);
                    }
                }).print();




        env.execute();



    }
}
