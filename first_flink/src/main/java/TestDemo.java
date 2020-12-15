import java.time.ZoneId;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
public class TestDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        // 添加Kafka数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.69.100:9092");  // Kafka服务器IP和端
        properties.setProperty("zookeeper.connect", "172.16.69.100:2181");  // Zookeeper服务器IP和端口
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(),
                properties);
        DataStream<String> stream = env.addSource(myConsumer);

        // 添加hadoop输出Sink
        stream.print();
        // 方式1：将数据导入Hadoop的文件夹
        //stream.writeAsText("hdfs://127.0.0.1:9000/flink/test");
        // 方式2：将数据导入Hadoop的文件夹
        BucketingSink<String> hadoopSink = new BucketingSink<>("hdfs://127.0.0.1:9000/flink");
        // 使用东八区时间格式"yyyy-MM-dd--HH"命名存储区
        hadoopSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai")));
        // 下述两种条件满足其一时，创建新的块文件
        // 条件1.设置块大小为100MB
        hadoopSink.setBatchSize(1024 * 1024 * 100);
        // 条件2.设置时间间隔20min
        hadoopSink.setBatchRolloverInterval(20 * 60 * 1000);
        // 设置块文件前缀
        hadoopSink.setPendingPrefix("");
        // 设置块文件后缀
        hadoopSink.setPendingSuffix("");
        // 设置运行中的文件前缀
        hadoopSink.setInProgressPrefix(".");
        // 添加Hadoop-Sink,处理相应逻辑
        stream.addSink(hadoopSink);

        env.execute("WordCount from Kafka data");
    }
}
