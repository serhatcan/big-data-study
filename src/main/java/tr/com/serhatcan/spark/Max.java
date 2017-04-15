package tr.com.serhatcan.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 * Created by Serhat Can on 4/15/17.
 */
public class Max {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("1807007-Part1").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);

        JavaRDD<String> input = sc.textFile("hdfs://localhost/user/cloudera/1807007/1807007.txt");


        JavaPairRDD<String, Double> pairs = input.mapToPair(s -> {
            String[] values = s.split(" ");
            return new Tuple2<>(values[0], Double.parseDouble(values[1]));
        });

        JavaPairRDD<String, Double> reducedPair = pairs.groupByKey().mapValues(new Function<Iterable<Double>, Double>() {
            double max = 0.0;
            @Override
            public Double call(Iterable<Double> values) throws Exception {

                values.forEach(v -> {
                    if(max < v) {
                        max = v;
                    }
                });
                return max;
            }
        });

        List<Tuple2<String, Double>> result = reducedPair.collect();

        result.forEach(System.out::println);

        sc.close();
        sc.stop();
    }
}
