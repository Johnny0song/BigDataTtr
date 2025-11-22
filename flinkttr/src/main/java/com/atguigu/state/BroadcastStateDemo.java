package com.atguigu.state;

import com.atguigu.pojos.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {

        // 方法1: 使用明确的端口配置
        Configuration config = new Configuration();

        // 重要：在 Mac 上使用这些配置
        config.setInteger(RestOptions.PORT, 8081);           // 设置端口
        config.setString(RestOptions.ADDRESS, "localhost");   // 绑定地址
        config.setString(RestOptions.BIND_ADDRESS, "localhost");
        config.setString(RestOptions.BIND_PORT, "8081");




        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());


   /*     // 代码中用到hdfs，需要导入hadoop依赖、指定访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 设置 Hadoop 配置目录
        System.setProperty("HADOOP_CONF_DIR",
                System.getProperty("user.dir") + "/src/main/resources");

        // 或者手动加载配置
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new Path("src/main/resources/core-site.xml"));
        hadoopConf.addResource(new Path("src/main/resources/hdfs-site.xml"));


*/


        System.setProperty("HADOOP_USER_NAME", "atguigu");
        String hadoopConfDir = System.getProperty("user.dir") + "/flinkttr/src/main/resources";


        System.setProperty("HADOOP_CONF_DIR", hadoopConfDir);
        System.out.println("Hadoop配置目录: " + hadoopConfDir);
        // ==================== 检查点配置 ====================

        env.enableCheckpointing(2000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/richard/IdeaProjects/BigDataTtr/flinkttr/data/checkpoint");
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop101:8020/flink/checkpoint");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop101", 9999);
        SingleOutputStreamOperator<WaterSensor> wsDs = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] values = value.trim().split(" ");
                WaterSensor waterSensor = new WaterSensor(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
                return waterSensor;
            }
        });



        DataStreamSource<String> dataStreamSourceConfig = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDsConfig = dataStreamSourceConfig.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] values = value.trim().split(" ");
                WaterSensor waterSensor = new WaterSensor(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
                return waterSensor;
            }
        });




        // TODO 1. 将 配置流 广播
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<WaterSensor> configBS = wsDsConfig.broadcast(broadcastMapState);

        wsDs.connect(configBS).process(new BroadcastProcessFunction<WaterSensor, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                System.out.println(broadcastState.get(value.getId()));
            }

            @Override
            public void processBroadcastElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                broadcastState.put(value.getId(), value.getVc());
            }
        }).print();


        env.execute();
    }
}
