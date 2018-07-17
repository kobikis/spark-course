package com.trainologic.spark.course.streaming;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;

public class StructuredStreamingSinks {
    public static void fileWriteout() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<String> words = netLines.as(Encoders.STRING())  //Make it Dataset
                .flatMap((FlatMapFunction<String, String>)
                                line -> Arrays.asList(line.split(" ")).iterator(),
                        Encoders.STRING());

        StreamingQuery query = words
                .writeStream()
                .format("json")
                .option("checkpointLocation", "src/main/resources/output/streaming/check_point")
                .option("path", "src/main/resources/output/streaming/coplete_count.json")
                .outputMode("append")
                .start();
        query.awaitTermination();

    }


    public static void triggering() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<String> words = netLines.as(Encoders.STRING())  //Make it Dataset
                .flatMap((FlatMapFunction<String, String>)
                                line -> Arrays.asList(line.split(" ")).iterator(),
                        Encoders.STRING());

        Dataset<Row> wcs = words.groupBy("value").count();

        StreamingQuery query = wcs
                .writeStream()
                .format("console")
                .outputMode("update")
//                .trigger(Trigger.ProcessingTime("10 seconds"))
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
        query.awaitTermination();

    }

    public static void queries() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<String> words = netLines.as(Encoders.STRING())  //Make it Dataset
                .flatMap((FlatMapFunction<String, String>)
                                line -> Arrays.asList(line.split(" ")).iterator(),
                        Encoders.STRING());

        Dataset<Row> wcs = words.groupBy("value").count();
        StreamingQuery query = wcs
                .writeStream()
                .format("console")
                .outputMode("update")
                .start();

        System.out.println(query.id());
        query.explain();
        System.out.println(query.lastProgress());
        query.awaitTermination();

    }

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//        fileWriteout();
        triggering();
//        queries();
    }
}
