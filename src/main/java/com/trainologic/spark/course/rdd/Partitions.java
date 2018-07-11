package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class Partitions {
    private static List<Integer> createRange(int num) {
        List<Integer> l = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            l.add(i);
        }
        return l;
    }

    public static void partitions() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        JavaRDD<Integer> intRDD = sc.parallelize(createRange(200));

        System.out.println(intRDD.partitions().size());

        JavaRDD<String> nameOfTheLength = intRDD.mapPartitions(partItr -> {
            int sum = 0;
            while (partItr.hasNext()) {
                sum += partItr.next();
            }
            return Collections.singletonList(sum + "").iterator();
        });
        System.out.println(nameOfTheLength.collect());

        sc.close();
    }

    public static void checkRegex1() {
        JavaSparkContext sc = SparkUtils.createSparkContext();

        //Read the file
        JavaRDD<String> linesRDD  = sc.textFile("src/main/resources/books/book1.txt");

        //Split into words
        JavaRDD<String> wordsRDD  = linesRDD.flatMap(line -> Arrays.asList(line.split("\\s")).iterator());

        System.out.println("We have " + wordsRDD.count() + " Words");

        Pattern pattern = Pattern.compile("\\w+");

        JavaRDD<Boolean> matchesRDD  = wordsRDD.map(word -> {
            return pattern.matcher(word).matches();
        });

        PU.print("matches", matchesRDD.filter(b -> !b));

        sc.close();
    }

    public static void checkRegex2() {
        JavaSparkContext sc = SparkUtils.createSparkContext();

        //Read the file
        JavaRDD<String> linesRDD  = sc.textFile("src/main/resources/books/book1.txt");

        //Split into words
        JavaRDD<String> wordsRDD  = linesRDD.flatMap(line -> Arrays.asList(line.split("\\s")).iterator());

        System.out.println("We have " + wordsRDD.count() + " Words");

        JavaRDD<String> matchesRDD  = wordsRDD.mapPartitions(wordItr -> {
            Pattern pattern = Pattern.compile("\\w+");
            List<String> mathcedList = new ArrayList<>();

            while (wordItr.hasNext()){
                String word = wordItr.next();
                if (pattern.matcher(word).matches()){
                    mathcedList.add(word);
                }
            }
            //Close resource if needed
            return mathcedList.iterator();
        });

        PU.print("matches", matchesRDD);

        sc.close();
    }

    public static void main(String[] args) throws IOException {
//        partitions();
        checkRegex1();
//        checkRegex2();
    }
}
