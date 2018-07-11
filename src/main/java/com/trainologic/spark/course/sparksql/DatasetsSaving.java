package com.trainologic.spark.course.sparksql;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class DatasetsSaving {
    public static void saveFormats() {
        SparkSession spark = SparkUtils.createSparkSession();
        Encoder<Revenue> revEncoder = Encoders.bean(Revenue.class);

        Dataset<Revenue> revenueDataset =
                spark
                        .read()
                        .option("header", true)
                        .csv("src/main/resources/sparksql/revenues.csv")
                        .as(revEncoder);


        revenueDataset.write().mode("overwrite").option("header", true).csv("src/main/resources/output/rev.csv");
        revenueDataset.write().mode("overwrite").json("src/main/resources/output/rev.json");
        revenueDataset.write().mode("overwrite").parquet("src/main/resources/output/rev.parquet");
    }

    public static void savePartitions() {
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> flightsDF =
                spark
                        .read()
                        .option("header", true)
                        .csv("src/main/resources/sparksql/flights.csv")
//                        .limit(100)
                ;

        System.out.println("Number of partitions: " + flightsDF.rdd().partitions().length);

        flightsDF.select("ArrTime",  "CarrierDelay", "Origin", "Month", "DayofMonth").repartition(10, col("DayofMonth")).write().mode("overwrite").parquet("src/main/resources/output/flights_10.parquet");

//        flightsDF.write().mode("overwrite").json("src/main/resources/output/flights.json");
//        flightsDF.coalesce(3).write().mode("overwrite").json("src/main/resources/output/flights_3.json");
//        flightsDF.repartition(10).write().mode("overwrite").json("src/main/resources/output/flights_10.json");
//        flightsDF.repartition(4, col("DayofMonth")).write().mode("overwrite").json("src/main/resources/output/flights_4.json");
    }

    public static void saveGroupBy() {
        SparkSession spark = SparkUtils.createSparkSession();
        Encoder<Revenue> revEncoder = Encoders.bean(Revenue.class);

        Dataset<Revenue> revenueDataset =
                spark
                        .read()
                        .option("header", true)
                        .csv("src/main/resources/sparksql/revenues.csv").as(revEncoder);

        PU.show("Revs: Int", revenueDataset);

        revenueDataset.write().mode("overwrite").partitionBy("category").json("src/main/resources/output/group_by.json");
        spark.read().json("src/main/resources/output/group_by.json").printSchema();
    }

    public static void modes() {
        SparkSession spark = SparkUtils.createSparkSession();
        Encoder<Revenue> revEncoder = Encoders.bean(Revenue.class);

        Dataset<Revenue> revenueDataset =
                spark
                        .read()
                        .option("header", true)
                        .csv("src/main/resources/sparksql/revenues.csv").as(revEncoder);

        PU.show("Revs: Int", revenueDataset);

        String path = "src/main/resources/output/modes.json";
        revenueDataset.write().mode("overwrite").json(path);
        spark.read().option("header", true).json(path).as(revEncoder).show();
        revenueDataset.write().mode("append").json(path);
        spark.read().option("header", true).json(path).as(revEncoder).show();


        //Ignore
//        revenueDataset.write().mode("overwrite").json(path);
////        spark.read().option("header", true).json(path).as(revEncoder).show();
//        revenueDataset.write().mode("ignore").json(path);
//        spark.read().option("header", true).json(path).as(revEncoder).show();
//        revenueDataset.write().mode(SaveMode.ErrorIfExists).json(path);
//        spark.read().option("header", true).json(path).as(revEncoder).show();
    }

    public static void main(String[] args) {

//        saveFormats();
        savePartitions();
//        saveGroupBy();
//        modes();
    }

}

