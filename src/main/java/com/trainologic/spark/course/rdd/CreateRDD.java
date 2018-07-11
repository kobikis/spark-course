package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CreateRDD {
    public static void fromCollection() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        List<Integer> ints = Arrays.asList(1, 2, 2, 3, 4, 4, 5, 5);

        JavaRDD<Integer> intRDD = sc.parallelize(ints);

        PU.print("Full list", intRDD);

        PU.print("Greater than 2", intRDD.filter(f -> f > 2));

        PU.print("Distinct", intRDD.distinct());

        PU.print("Exponent",
                intRDD.flatMap(f -> Arrays.asList(f, f * f).iterator()));
        System.out.println(intRDD.partitions().size());
        sc.close();
    }

    public static void fromFile(){
        JavaSparkContext sc = SparkUtils.createSparkContext();
        JavaRDD<String> bookRDD  = sc.textFile("src/main/resources/books/book1.txt");

        PU.print("The lines", bookRDD);

        PU.print("The lines", bookRDD
                .filter(line ->
                    !line.isEmpty() && Character.isLetter(line.codePointAt(0))
                ));

        System.out.println(bookRDD.count());
        System.out.println(bookRDD.filter(l -> !l.isEmpty()).count());
        sc.close();
    }

    public static void main(String[] args) throws IOException {
        fromCollection();
//        fromFile();
    }
}
