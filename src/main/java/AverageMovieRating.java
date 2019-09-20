import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Average movie rating.
 */
public class AverageMovieRating {
    public static void main(String[] args) throws FileNotFoundException {
        SparkConf conf = new SparkConf().setAppName("MyFirstSparkApp").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("FATAL");
        JavaRDD<String> rdd = sparkContext.textFile("src/main/resources/u.data");
        //works
        JavaPairRDD<Integer, Tuple2<Double, Double>> integerTuple2JavaPairRDD = rdd.mapToPair(x -> new Tuple2<Integer, Tuple2<Double, Double>>((Integer) Integer.valueOf(x.split("\t")[1]),
                new Tuple2<Double, Double>(Double.parseDouble(x.split("\t")[2]), 1.0))).reduceByKey((x, y) -> new Tuple2(x._1 + y._1, x._2 + y._2));

        ///Doesnt work while running.
        JavaPairRDD<Integer, Tuple2<Double, Double>> integerTuple2JavaPairRDD1 = rdd.mapToPair(x -> new Tuple2<Integer, Tuple2<Double, Double>>((Integer) Integer.valueOf(x.split("\t")[1]),
                new Tuple2<Double, Double>(Double.parseDouble(x.split("\t")[2]), 1.0))).reduceByKey((x, y) -> new Tuple2(x._1 + y._1, x._2 + y._2)).filter(x->x._2._2>10.0);

        JavaPairRDD<Integer, Tuple2<Double, Double>> filteredRDD = integerTuple2JavaPairRDD.filter(x->x._2._2>10.0);

        List<Tuple2<Double, Integer>> take = filteredRDD.mapToPair(x->new Tuple2(x._2._1/x._2._2,x._1)).sortByKey(false).take(10);
        take.forEach(x-> System.out.println(x._1+"::::"+x._2));

        File itemFile = new File("src/main/resources/u.item");
        Scanner scanner = new Scanner(itemFile);
        Map<Integer,String> map = new HashMap<Integer,String>(10);
        while(scanner.hasNextLine()){
            String s = scanner.nextLine();
            String[] split = s.split(";");
            map.put(Integer.parseInt(split[0]),split[1]);
        }

        take.forEach(x-> System.out.println(map.get(x._2)));
    }
}
