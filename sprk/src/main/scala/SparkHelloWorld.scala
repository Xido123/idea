import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparkHelloWorld {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHelloWorld")
    /*
    *
  *
    * get partitions before actions
    *
    * */
    val sc = new SparkContext(sconf)
    sc.setCheckpointDir("hdfs://ns1")
    val text:RDD[String] = sc.textFile("one.txt")
    println(text.partitions.length)
    val value: RDD[(String, Iterable[(String, Int)])] = text.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)

    /*
    *
    *   evey driver remember the checkpoint
    *   driver get the checkpoint
    *
    *
    * before value will forget
    * */
    value.checkpoint()
    val out = value.partitionBy(new HashPartitioner(11))

//      .mapValues(_.size).cache().collectAsMap()
    println(out)
//
  }

}
