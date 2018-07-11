package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Pi {


    public static void pi(int sampelSize){
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> l = new ArrayList<>(sampelSize);
        for (int i = 0; i < sampelSize; i++) {
            l.add(i);
        }
        long count = sc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / sampelSize);

        sc.close();
    }

    public static void main(String[] args) throws IOException {
        pi(1000000);
    }
}
