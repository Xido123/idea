import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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

        flatMap.map(new Function<String, Tuple2>() {
            @Override
            public Tuple2 call(String v1) throws Exception {
                return new Tuple2(v1,1);
            }
        });

    }

}
