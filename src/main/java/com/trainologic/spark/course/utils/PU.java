package com.trainologic.spark.course.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;

import java.util.Arrays;
import java.util.List;

public class PU {
    public static void print(String title, JavaRDD rdd) {
        print(title, rdd, 100);
    }

    public static void print(String title, JavaRDD rdd, int maxElem) {
        print(title, rdd.top(maxElem));
    }

    public static void print(String title, List list) {
        StringBuilder dashes = new StringBuilder();
        for (int i = 0; i < title.length() + 2; i++) {
            dashes.append("-");
        }
        System.out.println(new StringBuilder()
                .append("**************************\n")
                .append(title).append("\n")
                .append(dashes).append("\n")
                .append(Arrays.toString(list.toArray())).append("\n").append("\n")
                .append("**************************").append("\n"));
    }

    public static void show(String title, Dataset ds) {
        System.out.println(title);
        ds.show();
    }
}
