package com.trainologic.spark.course.streaming;

import com.trainologic.spark.course.sparksql.Revenue;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;

public class StructuredStreamingWindows {
    public static void windExample2() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> netLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<String> words = netLines.flatMap((FlatMapFunction<Row, String>)
                        row -> Arrays.asList(row.getString(0).split(" ")).iterator(),
                Encoders.STRING());

        Dataset<Row> wordsTs = words
                .withColumn("timestamp", lit(functions.current_timestamp()));

        Dataset<Row> aggRes = wordsTs.groupBy(
                functions.window(col("timestamp"), "20 seconds", "10 seconds"), col("value")
        ).count();

        StreamingQuery query = aggRes
                .orderBy("window", "value")
                .writeStream()
                .format("console")
                .outputMode("complete")
                .option("truncate", false)
                .start();
        query.awaitTermination();
        query.explain();

    }

    public static void windExample() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> rateLines = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
                .load();

        Dataset<Row> aggRes = rateLines.groupBy(
          functions.window(col("timestamp"), "20 seconds", "5 seconds"), col("value")
        ).count();

        StreamingQuery query = aggRes
                .writeStream()
                .format("console")
                .outputMode("update")
                .option("truncate", false)
                .start();
        query.awaitTermination();

    }

        public static void windLateExample() throws InterruptedException, StreamingQueryException {
        SparkSession spark = SparkUtils.createSparkSession();


        Dataset<Row> rateLines = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
                .load();

        Dataset<Row> aggRes = rateLines
                .withWatermark("timestamp", "1 minutes")
                .groupBy(
                functions.window(col("timestamp"), "20 seconds", "5 seconds"), col("value")
        ).count();

        StreamingQuery query = aggRes
                .writeStream()
                .format("console")
                .outputMode("update")
                .option("truncate", false)
                .start();
        query.awaitTermination();

    }





    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
        windExample2();
//        windExample();
    }
}
