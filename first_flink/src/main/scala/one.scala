
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object one {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = environment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092") // Kafka服务器IP和端

    properties.setProperty("zookeeper.connect", "localhost:2181") // Zookeeper服务器IP和端口

    properties.setProperty("group.id", "test")

    val streamKafka: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test",new SimpleStringSchema(),properties))

    val value: DataStream[String] = streamKafka.map((a:String)=>{
      a.substring(a.lastIndexOf("["),a.lastIndexOf("]")+1)
    })
//    val count = value.countWindowAll(0,10).sum(1)

    env.execute("first flink")
  }
}
