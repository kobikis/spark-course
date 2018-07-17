package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class StocksStreaming {
    public static void stocks() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<Stock> stocks = netLines.map((MapFunction<Row, Stock>) row -> {
                    String[] splited = row.getString(0).split(",");
                    return new Stock(
                            splited[0].trim().toLowerCase(),
                            Integer.parseInt(splited[1].trim().toLowerCase()),
                            new Timestamp(System.currentTimeMillis()));
                },
                Encoders.bean(Stock.class));



        StreamingQuery query =
                stocks.groupBy("name").max("price")
                        .writeStream()
                        .format("console")
                        .outputMode("complete")
                        .trigger(Trigger.ProcessingTime("10 seconds"))
                        .option("truncate", false)
                        .start();




        query.awaitTermination();

    }


    public static void stocks2() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<Stock> stocks = netLines.map((MapFunction<Row, Stock>) row -> {
                    String[] splited = row.getString(0).split(",");
                    return new Stock(
                            splited[0].trim().toLowerCase(),
                            Integer.parseInt(splited[1].trim().toLowerCase()),
                            new Timestamp(System.currentTimeMillis()));
                },
                Encoders.bean(Stock.class));

        Dataset<Row> stockAgg = stocks.groupBy(
                functions.window(col("ts"), "20 seconds", "10 seconds"), col("name")
        ).max("price").as("max_price");


        StreamingQuery query =
                stockAgg
                        .withWatermark("ts", "1 hour")
                        .dropDuplicates("name")
                        .orderBy("window", "name")
                        .writeStream()
                        .format("console")
                        .outputMode("complete")
                        .option("truncate", false)
                        .start();
        query.awaitTermination();

    }

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//        stocks();
        stocks2();
    }
}
