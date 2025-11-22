package com.atguigu.window;

import com.atguigu.pojos.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);


        env.setParallelism(1);

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


/*        wsDsWithWk.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(0)))
                        .aggregate(new AggregateFunction<WaterSensor, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> createAccumulator() {
                                return Tuple2.of("", 0);
                            }

                            @Override
                            public Tuple2<String, Integer> add(WaterSensor value, Tuple2<String, Integer> accumulator) {

                                return Tuple2.of(value.getId(), accumulator.f1 + value.getVc());
                            }

                            @Override
                            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                                return null;
                            }
                        }).print();*/

        OutputTag<WaterSensor> wsLater = new OutputTag<>("ws", TypeInformation.of(WaterSensor.class));

/*        SingleOutputStreamOperator<WaterSensor> reduce = wsDsWithWk.keyBy(WaterSensor::getId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                .sideOutputLateData(wsLater)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {

                        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }
                });
        reduce.getSideOutput(wsLater).print("late");
        reduce.print("main");*/


    /*    SingleOutputStreamOperator<WaterSensor> reduce = wsDsWithWk.keyBy(WaterSensor::getId)
        .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
        .sideOutputLateData(wsLater)
        .apply(new WindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensor> out) throws Exception {
                int count = 0;
                for (WaterSensor waterSensor : input) {
                    count += waterSensor.getVc();
                }
                WaterSensor next = input.iterator().next();

                out.collect(new WaterSensor(next.getId(),  next.ts , count));
            }
        });
        reduce.getSideOutput(wsLater).print("late");
        reduce.print("main");*/




/*        SingleOutputStreamOperator<WaterSensor> reduce = wsDsWithWk.keyBy(WaterSensor::getId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                .sideOutputLateData(wsLater)
                .process(new ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<WaterSensor> out) throws Exception {
                        int count = 0;
                        for (WaterSensor waterSensor : elements) {
                            count += waterSensor.getVc();
                        }
                        WaterSensor next = elements.iterator().next();

                        out.collect(new WaterSensor(next.getId(),  next.ts , count));
                    }
                });


        reduce.getSideOutput(wsLater).print("late");
        reduce.print("main");*/



/*
        wsDsWithWk.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(2L)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(2L)))
                        .reduce(new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                value2.setTs(value1.getTs() + value2.getTs());
                                return value2;
                            }
                        })
                .print();
*/




        env.execute();



    }
}
