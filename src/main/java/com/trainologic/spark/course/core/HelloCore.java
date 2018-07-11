package com.trainologic.spark.course.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class HelloCore {
    public static void helloMap(){
        SparkConf conf = new SparkConf().setAppName("Hello Spark").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("Hello", "Spark", "Java", "a", "b");
        JavaRDD<String> wordRDD = jsc.parallelize(data);

        JavaRDD<String> lowerCaseRDD = wordRDD.map(word -> word.toLowerCase());
        System.out.println(Arrays.toString(lowerCaseRDD.collect().toArray()));

        JavaRDD<String> lengthRDD = wordRDD.filter(word -> word.length() > 1);
        System.out.println(Arrays.toString(lengthRDD.collect().toArray()));


    }


    public static void main(String[] args) {
        helloMap();
    }
}
