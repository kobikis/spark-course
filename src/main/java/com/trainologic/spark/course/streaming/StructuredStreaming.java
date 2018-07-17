package com.trainologic.spark.course.streaming;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StructuredStreaming {
    public static void netSimple() throws InterruptedException, StreamingQueryException {
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

        StreamingQuery query = words
                .writeStream()
                .format("console")
                .outputMode("update")
                .start();
        query.awaitTermination();

    }

    public static void outputModeAppend() throws InterruptedException, StreamingQueryException {
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
                .format("console")
                .outputMode("append")
                .start();
        query.awaitTermination();

    }

    public static void outputModeUpdate() throws InterruptedException, StreamingQueryException {
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
        query.awaitTermination();

    }

    public static void readSocket() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9998")
                .load();

        Dataset<String> words = netLines.as(Encoders.STRING());  //Make it Dataset
//                .flatMap((FlatMapFunction<String, String>)
//                                line -> Arrays.asList(line.split(" ")).iterator(),
//                        Encoders.STRING());

        StreamingQuery query = words
                .writeStream()
                .format("console")
                .outputMode("append")
                .start();
        query.awaitTermination();

    }

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
        netSimple();
//        outputModeAppend();
//        outputModeUpdate();
//        readSocket();
    }
}
