import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
    val text:RDD[String] = sc.textFile("one.txt")
    val filter:RDD[String] = text.filter(_.contains("h"))
    val flatMap:RDD[String] = filter.flatMap(_.split(" "))
    val map:RDD[(String,Int)] = flatMap.map((_,1))
    val groupBy:RDD[(String,Iterable[(String,Int)])] = map.groupBy(_._1)
    val wordCount:RDD[(String,Int)] = groupBy.mapValues(_.size).cache()
    wordCount.foreach(println)
    val tuples :Array[(String,Int)] = wordCount.collect()
    tuples.foreach(println)
    wordCount.saveAsTextFile("a.txt")
  }

}
