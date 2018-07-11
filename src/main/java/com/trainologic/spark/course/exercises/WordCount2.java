package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class WordCount2 {
    public static void wordCount(){
        JavaSparkContext sc = SparkUtils.createSparkContext();

        //Read the file
        JavaRDD<String> linesRDD  = sc.textFile("src/main/resources/books/book1.txt");

        //Split into words
        JavaRDD<String> wordsRDD  = linesRDD.flatMap(line -> Arrays.asList(line.split("\\s")).iterator());

        //Map into tupple
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(word -> new Tuple2<>(word, 1));

        //Reduce by key
        JavaPairRDD<String, Integer> results = pairRDD.reduceByKey((a,b) -> a + b);

        System.out.println(Arrays.toString(results.filter(x -> x._2 > 100).collect().toArray()));


        sc.close();
    }

    public static void wordCount2(){
        JavaSparkContext sc = SparkUtils.createSparkContext();

        //Read the file
        JavaRDD<String> linesRDD  = sc.textFile("src/main/resources/books/book1.txt");
        Set<String> stopWords = new HashSet<>(Arrays.asList("yes", "no", "ok", "in", "you", "a", "to", "and", "the"));

        Broadcast<Set<String>> ignroedBC = sc.broadcast(stopWords);

        LongAccumulator yes = sc.sc().longAccumulator("legal_word");
        LongAccumulator no = sc.sc().longAccumulator("illegal_word");
        LongAccumulator totalWords = sc.sc().longAccumulator("total_words");
        LongAccumulator totalLines = sc.sc().longAccumulator("totel_lines");

        //Split into words
        JavaRDD<String> wordsRDD  = linesRDD.mapPartitions(line -> {
                    Pattern pattern = Pattern.compile("\\w+");
                    List<String> output = new ArrayList<>();

                    totalLines.add(1);
                    while (line.hasNext()){
                       String lineStr = line.next();
                       String[] words = lineStr.split(" ");

                       totalWords.add(words.length);
                       for (String str: words){
                           if (!ignroedBC.getValue().contains(str) && pattern.matcher(str).matches()){
                               yes.add(1);
                               output.add(str);
                           }else{
                               no.add(1);
                           }
                       }

                    }
                    return output.iterator();
                });

        //Map into tupple
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(word -> new Tuple2<>(word, 1));

        //Reduce by key
        JavaPairRDD<String, Integer> results = pairRDD.reduceByKey((a,b) -> a + b);

        System.out.println(Arrays.toString(results.filter(x -> x._2 > 50).collect().toArray()));

        System.out.println("Total lines: " + totalLines.value());
        System.out.println("Total words: " + totalWords.value());
        System.out.println("Legal words: " + yes.value());
        System.out.println("Illegal words: " + no.value());

        sc.close();
    }


    public static void main(String[] args) throws IOException {
        wordCount2();
    }
}
