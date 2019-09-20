import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class PairRDDDemonstration {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairRDDDemonstration").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setLogLevel("FATAL");
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("Java", "Hadoop", "Pig", "Spark", "Java", "Hadoop", "Pig", "Spark", "Java", "Hadoop", "Pig", "Spark"));
            Map<String, Integer> stringIntegerMap = rdd.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y).collectAsMap();
        System.out.println(stringIntegerMap.toString());
    }
}
