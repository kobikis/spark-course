package com.trainologic.spark.course.streaming;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SimpleStreaming {
    public static void fromFile() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Streaming Test").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));

        JavaDStream<String> dstream = jssc.textFileStream
                ("/home/ophchu/companies/trainologic/repos/amdocs-java-spark-course-private/src/main/resources/streaming/infiles/");
        JavaDStream<Integer> toIntsdStream = dstream.map(Integer::parseInt);
        toIntsdStream.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public static void socketStreaming() throws InterruptedException {
        System.out.println("Creating...");
        JavaStreamingContext jssc = SparkUtils.createStreamingContext();

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        words.print();
        jssc.start();
        System.out.println("Started...");
        jssc.awaitTermination();
    }

    public static void socketStreamingReduce() throws InterruptedException {
        System.out.println("Creating...");
        JavaStreamingContext jssc = SparkUtils.createStreamingContext();

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaDStream<Integer> wordsLength = lines.map(word -> word.length());
        JavaDStream<Integer> lengthSum = wordsLength.reduce((len1, len2) -> len1 + len2);


        words.print();
        lengthSum.print();
        jssc.start();
        System.out.println("Started...");
        jssc.awaitTermination();
    }

    public static void socketStreamingTransformation() throws InterruptedException {
        System.out.println("Creating...");
        JavaStreamingContext jssc = SparkUtils.createStreamingContext();

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaDStream<String> unioned = words.transform(rdd -> rdd.union(rdd));

        unioned.print();
        jssc.start();
        System.out.println("Started...");
        jssc.awaitTermination();
    }

    public static void updateStateByKey() throws InterruptedException {

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    Integer newSum = state.or(0);
                    for (int i: values){
                        newSum += i;
                    }
                    return Optional.of(newSum);
                };
        System.out.println("Creating...");
        JavaStreamingContext jssc = SparkUtils.createStreamingContext();
        jssc.checkpoint("src/main/resources/streaming/checkpoints/");


        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> countWords = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> results = countWords
                .reduceByKey((w1, w2) -> w1 + w2)
                .updateStateByKey(updateFunction);


        results.print();
        jssc.start();
        System.out.println("Started...");
        jssc.awaitTermination();
    }




    public static void main(String[] args) throws InterruptedException {
        fromFile();

//        socketStreaming();
//        socketStreamingReduce();
//        socketStreamingTransformation();
//        updateStateByKey();
    }
}
