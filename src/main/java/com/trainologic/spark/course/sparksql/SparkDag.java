package com.trainologic.spark.course.sparksql;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
public class SparkDag {


    public static void simpleDAG() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Revenue> revenueDS = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/revenues.csv").as(Encoders.bean(Revenue.class));

//        revenueDS.explain(true);
//        revenueDS.filter("revenue > 5000").filter("revenue = '3000'").explain(true);
//        System.out.println("***************************************");
//        revenueDS.filter("revenue > 5000").select("product").explain(true);
//        System.out.println("***************************************");
//
//        revenueDS.select("product").filter("revenue > 5000").explain(true);

    }

    public static void complextDAG() {
        JavaSparkContext jsc = SparkUtils.createSparkContext();
        JavaPairRDD<String, Integer> positive =
                jsc.textFile("src/main/resources/sparksql/positive.txt")
                        .mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> bookRDD =
                jsc.textFile("src/main/resources/books/book1.txt")
                        .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Tuple2<Integer, Integer>> onlyPositive = bookRDD.join(positive);

        System.out.println(onlyPositive.toDebugString());
//        System.out.println(onlyPositive.collectAsMap().toString());

    }


    public static void dsDAG(){
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> airportsDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/airports.csv");

        Dataset<Row> countByCountry = airportsDF
                .groupBy("country")
                .count()
                .repartition(10, col("country"));

        countByCountry.explain(true);
        System.out.println(countByCountry.rdd().partitions().length);
    }

    public static void dsParquet(){
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> airportsDF = spark
                .read()
                .parquet("src/main/resources/output/flights_10.parquet");

        airportsDF.select("origin", "Month", "DayofMonth")
                .filter("DayOfMonth=2").explain(true);
//        airportsDF.show();
        System.out.println(airportsDF.rdd().partitions().length);
    }


    public static void dsJoin(){
        SparkSession spark = SparkUtils.createSparkSession();

        Dataset<Row> airportsDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/airports.csv");

        Dataset<Row> countByCountry = airportsDF.limit(10).join(airportsDF, "country");

        countByCountry.show();
        countByCountry.explain(true);

        countByCountry.count();
        System.out.println(countByCountry.rdd().partitions().length);
    }


    public static void main(String[] args) {

//        simpleDAG();
//        complextDAG();
//        dsDAG();
//        dsJoin();
        dsParquet();
    }
}
