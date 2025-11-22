package com.atguigu.state;

import com.atguigu.pojos.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateDemo {
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

        // 配置状态 TTL 使用事件时间
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10L)) // 10秒TTL
                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // ⭐关键配置
                .cleanupFullSnapshot() // 清理策略
                .build();

        wsDsWithWk
                .keyBy(WaterSensor::getId)
        .flatMap(new RichFlatMapFunction<WaterSensor, WaterSensor>() {

            // TODO 1.定义状态
            ValueState<String> lastVcState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-vc", String.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<WaterSensor> out) throws Exception {
                lastVcState.update(value.getId());
                System.out.println(lastVcState.value());
            }
        }).print();




        env.execute();



    }
}
