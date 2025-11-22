package com.atguigu.spark.sparkcore.rdd.serializeable;

import com.atguigu.spark.sparkcore.rdd.beans.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;

import java.util.Arrays;

public class Test01_Ser {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        User user1 = new User("zhangsan", 18);
        User user2 = new User("lisi", 19);
        User user3 = new User("wangwu", 19);

        sc.parallelize(Arrays.asList(user1, user2, user3), 2)
                .map(new Function<User, User>() {
                    @Override
                    public User call(User v1) throws Exception {
                        return new User(v1.getName(),  v1.getAge() + 2);
                    }
                })
                        .collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
