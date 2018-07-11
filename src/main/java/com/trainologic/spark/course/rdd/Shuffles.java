package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Shuffles {
    public static void groupBy() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> intsRDD = sc.parallelize(ints);

        JavaPairRDD<Integer, Iterable<Integer>> groupByMod3 = intsRDD.groupBy(i -> i % 3);

        System.out.println(groupByMod3.collectAsMap());

        sc.close();
    }

    public static void reduceByKey() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> intsRDD = sc.parallelize(ints);
        JavaPairRDD<Integer, Integer> intsModRDD = intsRDD.mapToPair(i -> new Tuple2<>(i%3, i));
        JavaPairRDD<Integer, Integer> reduceRDD = intsModRDD.reduceByKey((v1, v2) -> v1 + v2);

        PU.print("Reduce Mod 0", reduceRDD.lookup(0));
        PU.print("Reduce Mod 1", reduceRDD.lookup(1));
        PU.print("Reduce Mod 2", reduceRDD.lookup(2));


        sc.close();
    }

    public static void join() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> smallInts = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> bigInts = Arrays.asList(11, 12, 13, 14, 15, 16);

        JavaPairRDD<Integer, Integer> smallIntsModRDD =
                sc.parallelize(smallInts).mapToPair(i -> new Tuple2<>(i%3, i));

        JavaPairRDD<Integer, Integer> bigIntsModRDD =
                sc.parallelize(bigInts).mapToPair(i -> new Tuple2<>(i%3, i));


        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = smallIntsModRDD.join(bigIntsModRDD);

        PU.print("as", smallIntsModRDD.lookup(0));
        PU.print("as", bigIntsModRDD.lookup(0));
        PU.print("as", joinedRDD.lookup(0));


        sc.close();
    }

    public static void main(String[] args) throws IOException {
//        groupBy();
//        reduceByKey();
        join();
    }

}
