import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MyFirstJavaProgram {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MyFirstSparkApp").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("FATAL");
        JavaRDD<Integer> integerRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
//        integerRDD.foreach(i-> System.out.println(i));
//        List<Integer> integerList = integerRDD.collect();
//        integerList.forEach(i-> System.out.println(i));

//        integerRDD.map(x -> x * x).collect().forEach(System.out::println);
        integerRDD.filter(x->x%2==0).collect().forEach(System.out::println);
    }
}
