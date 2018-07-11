package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Pi2 {


    public static void pi(int sampelSize){
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> l = new ArrayList<>(sampelSize);
        for (int i = 0; i < sampelSize; i++) {
            l.add(i);
        }
        JavaRDD<Integer> points = sc.parallelize(l);
        JavaRDD<Double> res = points.map(i -> Math.pow(Math.random(), 2)  + Math.pow(Math.random(), 2));

        sc.close();
    }

    public static void main(String[] args) throws IOException {
        pi(1000000);
    }
}
