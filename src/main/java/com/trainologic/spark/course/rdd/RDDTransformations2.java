package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RDDTransformations2 {

    public static void moreTransformation() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> ints2 = Arrays.asList(1, 2);

        JavaRDD<Integer> intRDD = sc.parallelize(ints);
        JavaRDD<Integer> int2RDD = sc.parallelize(ints2);

        PU.print("Sample", intRDD.sample(false, 0.3));
        PU.print("Sample", intRDD.sample(false, 0.2));

        PU.print("Union", intRDD.union(intRDD));
        PU.print("Intersect", intRDD.intersection(int2RDD));
        PU.print("Distinct", intRDD.union(intRDD).distinct());

        sc.close();
    }

    public static void groupBy() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);

        JavaPairRDD<String, Integer> langLengRDD = langsRDD.flatMapToPair(lang ->
                Arrays.asList(
                        new Tuple2<>(lang, 1),
                        new Tuple2<>(lang, 2),
                        new Tuple2<>(lang, 3),
                        new Tuple2<>(lang, 4)
                ).iterator()
        );

        System.out.println(Arrays.toString(langLengRDD.collect().toArray()));

        //Group by key
        JavaPairRDD<String, Iterable<Integer>> grouped = langLengRDD.groupByKey();

        System.out.println(grouped.collectAsMap());


        //Reduce by key
        JavaPairRDD<String, Integer> reduced = langLengRDD.reduceByKey((k, v) -> k + v);
        System.out.println(reduced.collectAsMap());


        sc.close();
    }

    public static void aggBy() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);

        JavaPairRDD<String, String> langLengRDD = langsRDD.flatMapToPair(lang ->
                Arrays.asList(
                        new Tuple2<>(lang, lang + "_1" + lang),
                        new Tuple2<>(lang, lang + "_2" + lang),
                        new Tuple2<>(lang, lang + "_3" + lang),
                        new Tuple2<>(lang, lang + "_4" + lang)
                ).iterator()
        );

        System.out.println(Arrays.toString(langLengRDD.collect().toArray()));

        //Agg by key
        JavaPairRDD<String, Integer> lengthAgg =
                langLengRDD
                        .aggregateByKey(
                                0,
                                (Function2<Integer, String, Integer>) (v1, v2) -> v1 + v2.length(),
                                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        System.out.println(lengthAgg.collectAsMap());

        System.out.println(lengthAgg.sortByKey().collect());

        sc.close();
    }

    public static void main(String[] args) throws IOException {
//        moreTransformation();
//        groupBy();
        aggBy();

    }
}
