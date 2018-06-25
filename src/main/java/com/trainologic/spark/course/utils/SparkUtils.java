package com.trainologic.spark.course.utils;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkUtils {
    public static JavaSparkContext createSparkContext() {
        return SparkUtils.createSparkContext("test", "local[*]");
    }

    public static JavaSparkContext createSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        return jsc;
    }

    public static JavaStreamingContext createStreamingContext() {
        return createStreamingContext(10);
    }

    public static JavaStreamingContext createStreamingContext(long duration) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        return new JavaStreamingContext(conf, Durations.seconds(duration));
    }

    public static SparkSession createSparkSession(){
        return SparkSession
                .builder()
                .appName("test")
                .master("local[*]")
                .getOrCreate();
    }
}
