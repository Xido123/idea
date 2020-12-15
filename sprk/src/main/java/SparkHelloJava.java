import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class SparkHelloJava {
    public static void main(String[] args) {
        SparkConf sparkConf =  new SparkConf().setMaster("local[*]").setAppName("SparkHelloJava");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = jsc.textFile("one.txt");

        JavaRDD<String> filter = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("h");
            }
        });

        JavaRDD<String> flatMap =  filter.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                String[]s1 =  s.split(" ");
                for(String ss:s1){
                    list.add(ss);
                }
                return list.iterator();
            }
        });

        JavaRDD<Tuple2<String,Integer> > map =  flatMap.map(new Function<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> call(String v1) throws Exception {
                return new Tuple2<>(v1,1);
            }
        });

        JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> groupBy =  map.groupBy(new Function<Tuple2<String, Integer>, String>() {

            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1;
            }
        });

        JavaPairRDD<String,Integer> wordCount =  groupBy.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Integer>() {
            @Override
            public Integer call(Iterable<Tuple2<String, Integer>> v1) throws Exception {
                Integer count = 0;
//                Iterator<Tuple2<String, Integer>> iterator = v1.iterator();
//
//                while(iterator.hasNext()){
//                    count++;
//                }
                for(Iterator<Tuple2<String,Integer>> iterator = v1.iterator();iterator.hasNext();){
                    iterator.next();
                    count++;
                }
                return count;

            }
        });

        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.err.println(stringIntegerTuple2);
            }
        });


    }

}

