import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class HadoopDependencyChecker {
    public static void main(String[] args) {
        try {
            System.out.println("=== Hadoop 依赖检查 ===");
            System.setProperty("HADOOP_USER_NAME", "atguigu");

            // 1. 检查基础类
            Class.forName("org.apache.hadoop.conf.Configuration");
            System.out.println("✅ org.apache.hadoop.conf.Configuration 找到");

            Class.forName("org.apache.hadoop.fs.FileSystem");
            System.out.println("✅ org.apache.hadoop.fs.FileSystem 找到");

            Class.forName("org.apache.hadoop.hdfs.HdfsConfiguration");
            System.out.println("✅ org.apache.hadoop.hdfs.HdfsConfiguration 找到");

            Class.forName("org.apache.hadoop.hdfs.DistributedFileSystem");
            System.out.println("✅ org.apache.hadoop.hdfs.DistributedFileSystem 找到");

            // 2. 测试配置加载
            Configuration conf = new Configuration();
            conf.addResource("data/core-site.xml");
            conf.addResource("data/hdfs-site.xml");
            System.out.println("✅ Hadoop 配置加载成功");

            // 3. 测试文件系统
            String hdfsUri = "hdfs://hadoop101:8020";
//            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:8020"), conf, "atguigu");
            System.out.println("✅ HDFS 文件系统连接成功: " + fs.getClass().getName());

            // 4. 测试基本操作
            boolean exists = fs.exists(new org.apache.hadoop.fs.Path("/"));
            System.out.println("✅ HDFS 根目录访问: " + (exists ? "成功" : "失败"));

            fs.close();
            System.out.println("\n🎉 所有 Hadoop 依赖检查通过！");

        } catch (ClassNotFoundException e) {
            System.err.println("❌ 缺少 Hadoop 类: " + e.getMessage());
            System.err.println("请检查 pom.xml 中是否添加了 hadoop-client、hadoop-hdfs 依赖");
        } catch (NoClassDefFoundError e) {
            System.err.println("❌ 类定义错误: " + e.getMessage());
            System.err.println("可能存在依赖冲突，尝试清理 Maven 缓存: mvn clean");
        } catch (Exception e) {
            System.err.println("❌ 其他错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
