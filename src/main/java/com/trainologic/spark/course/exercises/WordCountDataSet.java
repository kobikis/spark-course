package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WordCountDataSet {
    public static class WordCount implements Serializable {
        private String word;
        private Long count;

        public WordCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }
    }

    public static void wordCount() {
        SparkSession spark = SparkUtils.createSparkSession();

        //Read the file
        Dataset<String> linesDS = spark.read().textFile("src/main/resources/books/book1.txt");

        linesDS.show();
        //Split into words
        Dataset<WordCount> wordsCoundDS = linesDS.flatMap((FlatMapFunction<String, WordCount>) s -> {
            String[] splited = s.split(" ");
            List<WordCount> l = new ArrayList<>();
            for (String split : splited) {
                if (split.length() > 0 && Character.isAlphabetic(split.charAt(0))) {
                    l.add(new WordCount(split, 1l));
                }
            }
            return l.iterator();
        }, Encoders.bean(WordCount.class));


        wordsCoundDS.printSchema();
        wordsCoundDS.show();
        spark.close();
    }

    public static void main(String[] args) throws IOException {
        wordCount();
    }
}
