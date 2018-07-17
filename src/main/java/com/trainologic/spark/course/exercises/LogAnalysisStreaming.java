package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.sources.In;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class LogAnalysisStreaming {
    private static Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
            (values, state) -> {
                Integer newSum = state.or(0);
                for (int i: values){
                    newSum += i;
                }
                return Optional.of(newSum);
            };
    public static void analyzeLogs() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Streaming Test").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));

        jssc.sparkContext().setLogLevel("ERROR");
        jssc.checkpoint("src/main/resources/streaming/checkpoints2/");
        JavaDStream<String> dstream = jssc
                .textFileStream("/home/ophchu/companies/trainologic/repos/amdocs-java-spark-course-private/src/main/resources/streaming/inlogs/");
        JavaDStream<String> logLevelDS = dstream
                .filter(line -> line.startsWith("17"))
                .map(line -> line.split(" ")[2]);

        JavaPairDStream<String, Integer> pairDStream = logLevelDS.mapToPair(loglevel -> new Tuple2<>(loglevel, 1));

        JavaPairDStream<String, Integer> loglevelCount = pairDStream.reduceByKey((v1, v2) -> v1 + v2);

        JavaPairDStream<String, Integer> loglevelCountFull = loglevelCount.updateStateByKey(updateFunction);
        loglevelCount.print();
        loglevelCountFull.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public static void analyzeLogs2() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Streaming Test").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));

        JavaDStream<String> dstream = jssc
                .textFileStream("/home/ophchu/companies/trainologic/repos/amdocs-java-spark-course-private/src/main/resources/streaming/inlogs/");
        JavaDStream<String> logLevelDS = dstream
                .filter(line -> line.startsWith("17"))
                .map(line -> line.split(" ")[2]);

        JavaPairDStream<String, Long> loglevelCount = logLevelDS.countByValue();

        loglevelCount.print();
        jssc.start();
        jssc.awaitTermination();
    }


    public static void main(String[] args) throws InterruptedException {
        analyzeLogs();
//        analyzeLogs2();
    }
}
