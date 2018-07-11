package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

public class WordCount {

    public static void wordCount(){
        JavaSparkContext sc = SparkUtils.createSparkContext();

        //Read the file
        JavaRDD<String> linesRDD  = sc.textFile("src/main/resources/books/book1.txt");

        //Split into words
        JavaRDD<String> wordsRDD  = linesRDD.flatMap(line -> Arrays.asList(line.split("\\s")).iterator());

        //Map into tuple
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(word -> new Tuple2<>(word, 1));

        //Reduce by key
        JavaPairRDD<String, Integer> results = pairRDD.reduceByKey((a,b) -> a + b);

        results.cache();
        System.out.println(Arrays.toString(results.filter(x -> x._2 > 100).collect().toArray()));

        System.out.printf("");
        sc.close();
    }

    public static void wordCountMoreFunctional(){
        JavaSparkContext sc = SparkUtils.createSparkContext();

        //Read the file
        JavaPairRDD<String, Integer> results  =
                sc.textFile("src/main/resources/books/book1.txt")
                .flatMap(line -> Arrays.asList(line.split("\\s")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);

        System.out.println(Arrays.toString(results.filter(x -> x._2 > 100).collect().toArray()));


        sc.close();
    }

    public static void main(String[] args) throws IOException {
        wordCount();
//        wordCountMoreFunctional();
    }
}
