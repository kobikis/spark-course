package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RDDActions {

    public static void collectCount() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);


        //Map to length - nothing happen!
        JavaRDD<Integer> lengthsRDD = langsRDD.map(pl -> pl.length());

        //Now something is happening!
        PU.print("Collect", lengthsRDD.collect());

        //count:
        System.out.println("Length count: " + lengthsRDD.count());
        //First:
        System.out.println("Length first: " + lengthsRDD.first());
        //Take:
        PU.print("Take 3: ", lengthsRDD.take(3));


        sc.close();
    }

    public static void foreach() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<String> ints = Arrays.asList("Scala", "Java", "Python", "C++", "PHP", "Ruby", "Haskell");
        JavaRDD<String> langsRDD = sc.parallelize(ints);


        langsRDD.foreach(pl -> System.out.println("FE: " + pl));

        sc.close();
    }

    public static void main(String[] args) throws IOException {

//        collectCount();
        foreach();

    }
}
