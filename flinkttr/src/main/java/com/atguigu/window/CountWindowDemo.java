package com.atguigu.window;

import com.atguigu.pojos.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);


        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> wsDs = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] values = value.trim().split(",");
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
                .countWindow(3,1)
                .sum("vc")
                .print();

        env.execute();
    }
}
