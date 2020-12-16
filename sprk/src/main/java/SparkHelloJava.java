import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;

import java.util.Collections;
import java.util.List;


public class SparkHelloJava {
    public static void main(String[] args) {
        SparkConf sparkConf =  new SparkConf().setMaster("local[*]").setAppName("SparkHelloJava");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = jsc.textFile("one.txt");

        JavaRDD<String> filter = stringJavaRDD.filter((Function<String, Boolean>) v1 -> v1.contains("h"));

        JavaRDD<String> flatMap =  filter.flatMap((FlatMapFunction<String, String>) s -> {
            ArrayList<String> list = new ArrayList<>();
            String[]s1 =  s.split(" ");
            Collections.addAll(list, s1);
            return list.iterator();
        });

        JavaRDD<Tuple2<String,Integer> > map =  flatMap.map((Function<String, Tuple2<String, Integer>>) v1 -> new Tuple2<>(v1,1));

        JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> groupBy =  map.groupBy((Function<Tuple2<String, Integer>, String>) v1 -> v1._1);

        JavaPairRDD<String,Integer> wordCount =  groupBy.mapValues((Function<Iterable<Tuple2<String, Integer>>, Integer>) v1 -> {
            int count = 0;
            for (Tuple2<String, Integer> ignored : v1) {
                count++;
            }
            return count;

        });

        wordCount.foreach((VoidFunction<Tuple2<String, Integer>>) System.out::println);

        List<Tuple2<String,Integer>> collection = wordCount.collect();
        System.out.println(collection);
    }

}

