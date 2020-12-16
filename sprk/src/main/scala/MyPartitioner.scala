import org.apache.spark.Partitioner

class MyPartitioner extends Partitioner {
  override def numPartitions: Int = 11

  override def getPartition(key: Any): Int = {

    if(key.asInstanceOf[String].contains("hainiu")) 0 else key.asInstanceOf[String].hashCode % numPartitions


  }

}
