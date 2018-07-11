package com.trainologic.spark.course.utils;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;
import java.util.List;

public class SparkUtils {
    public static JavaSparkContext createSparkContext() {
        return SparkUtils.createSparkContext("test", "local[*]");
    }

    public static JavaSparkContext createSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
        return jsc;
    }

    public static JavaStreamingContext createStreamingContext() {
        return createStreamingContext(10);
    }

    public static JavaStreamingContext createStreamingContext(long duration) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        JavaStreamingContext jssc =  new JavaStreamingContext(conf, Durations.seconds(duration));
        jssc.sparkContext().setLogLevel("ERROR");
        return jssc;
    }

    public static SparkSession createSparkSession(){
        SparkSession spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        return spark;
    }

    public static List<Integer> createIntRange(int num) {
        List<Integer> l = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            l.add(i);
        }
        return l;
    }

    public static List<String> createStringRange(int num) {
        List<String> l = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            l.add(i + "_range");
        }
        return l;
    }

}
