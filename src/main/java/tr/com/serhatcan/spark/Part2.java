package tr.com.serhatcan.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Serhat Can on 4/15/17.
 */
public class Part2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("1807007-Part2").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);

        // Operation1: Read from HDFS
        JavaRDD<String> input = sc.textFile("hdfs://localhost/user/cloudera/1807007/1807007.txt");

        // Operation2: MapToPair
        JavaPairRDD<String, Double> pairs = input.mapToPair(s -> {
            String[] values = s.split(" ");
            return new Tuple2<>(values[0], Double.parseDouble(values[1]));
        }).persist(StorageLevel.MEMORY_ONLY());  // set storage level to MEMORY_ONLY

        // Operation 3.1: Filter-1
        JavaPairRDD<String, Double> filter1 = pairs.filter(tuple -> tuple._2() > 10).persist(StorageLevel.MEMORY_AND_DISK());
        // Operation 3.2: Filter-2
        JavaPairRDD<String, Double> filter2 = pairs.filter(tuple -> tuple._2() < 20).persist(StorageLevel.MEMORY_AND_DISK());


        // Operation 4: Intersection
        JavaPairRDD<String, Double> intersection = filter1.intersection(filter2);

        // Operation 4: SortByKey - Ascending Order
        JavaPairRDD<String, Double> sorted = intersection.sortByKey(true).persist(StorageLevel.DISK_ONLY());


        JavaPairRDD<String, Double> medianValues = sorted.groupByKey().mapValues(new Function<Iterable<Double>, Double>() {
            int count = 0;

            @Override
            public Double call(Iterable<Double> values) throws Exception {
                Collection<Double> valuesC = ((Collection<Double>) values);
                Double[] doubleArray = new Double[valuesC.size()];
                doubleArray = ((Collection<Double>) values).toArray(doubleArray);

                double median;
                if (doubleArray.length % 2 == 0)
                    median = ((double) doubleArray[doubleArray.length / 2] + (double) doubleArray[doubleArray.length / 2 - 1]) / 2;
                else
                    median = (double) doubleArray[doubleArray.length / 2];

                return median;
            }
        });


        List<Tuple2<String, Double>> medians = medianValues.collect();
        List<Tuple2<String, Double>> first3 = sorted.take(3);

        List<Tuple2<String, Double>> result = Stream.concat(medians.stream(), first3.stream()).collect(Collectors.toList());

        sc.parallelize(result).saveAsTextFile("hdfs://localhost/user/cloudera/1807007/1807007-result.txt");

        sc.close();
        sc.stop();
    }
}
