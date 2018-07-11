package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RDDTransformations {

    public static void map() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);

        PU.print("Full RDD", langsRDD);

        //Map
        JavaRDD<Integer> lengthsRDD = langsRDD.map(pl -> pl.length());
        PU.print("Length RDD", lengthsRDD);

        JavaRDD<Integer> lengthsRDD2 = langsRDD.map(String::length);
        PU.print("Length RDD", lengthsRDD2);

        //Map II
        JavaRDD<Double> doubledLengthsRDD = langsRDD.map(pl -> (double) pl.length() * pl.length());
        PU.print("Dobule Length RDD", doubledLengthsRDD);

        //Tuples
        //Pairs
        JavaPairRDD<String, Integer> nameLengthRDD =
                langsRDD.mapToPair(pl -> new Tuple2<>(pl, pl.length()));
        System.out.println("Name and length tuple\n" + nameLengthRDD.collect());


        sc.close();
    }

    public static void map2() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);

        PU.print("Full RDD", langsRDD);

        //You can use function:
        Function<String, String> howGoodLang = new Function<String, String>() {
            @Override
            public String call(String s){
                if ("scala".equals(s.toLowerCase())){
                    return "Scala is a great language!";
                }else if ("Java".equals(s.toLowerCase())){
                    return "Aha, Java is fine";
                }else {
                    return s + " is fine, not more";
                }
            }
        };

        JavaRDD<String> howGoodRDD = langsRDD.map(howGoodLang);
        PU.print("How Good", howGoodRDD);

        //Or in Lambda:
        Function<String, String> howGoodLangLambda = (Function<String, String>) s -> {
            if ("scala".equals(s.toLowerCase())){
                return "Scala is a great language!";
            }else if ("Java".equals(s.toLowerCase())){
                return "Aha, Java is fine";
            }else {
                return s + " is fine - not more";
            }
        };

        JavaRDD<String> howGoodLambdaRDD = langsRDD.map(howGoodLangLambda);
        PU.print("How Good", howGoodLambdaRDD);

        sc.close();
    }

    public static void filter() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);

        PU.print("Full RDD", langsRDD);

        //Filter I
        JavaRDD<String> withAOnlyRDD = langsRDD.filter(pl -> pl.contains("a"));
        PU.print("Contains a", withAOnlyRDD);

        //Filter II
        JavaRDD<String> lengthsRDD = langsRDD.filter(pl -> pl.length() > 3 && pl.contains("a"));
        PU.print("Length > 3 and contains a", lengthsRDD);

        //Find the length of those who has "a"
        JavaRDD<Integer> lengthOfAsRDD =
                langsRDD
                        .filter(pl -> pl.contains("a"))
                        .map(pl -> pl.length());
        PU.print("Length of w/ A", lengthOfAsRDD);


        sc.close();
    }

    public static void flatMap() {
        JavaSparkContext sc = SparkUtils.createSparkContext();

        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> intsRDD = sc.parallelize(ints);

        JavaRDD<Integer> multiGt2RDD = intsRDD.flatMap(i -> {
            if (i > 3){
                return Arrays.asList(i, i * i, i * i * i).iterator();
            }else{
                return Collections.EMPTY_LIST.iterator();
            }
        });
        PU.print("Multi GT 2", multiGt2RDD);

        sc.close();
    }

    public static void main(String[] args) throws IOException {

//        map();
//        map2();
//        filter();
        flatMap();

    }
}
