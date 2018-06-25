package com.trainologic.spark.course.precourse;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HelloSpark {
    public static void helloSpark(){
        SparkConf conf = new SparkConf().setAppName("Hello Spark").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> intsData = jsc.parallelize(data);

        System.out.println("Hello Spark!");
        System.out.println("Hello first distributed RDD:");
        System.out.println(Arrays.toString(intsData.collect().toArray()));

        System.out.println("Hello first distributed computation. Sum:");
        long sum = intsData.reduce((i1, i2) -> i1 + i2);
        System.out.println(sum);

    }

    public static void main(String[] args) {
        helloSpark();
    }
}
