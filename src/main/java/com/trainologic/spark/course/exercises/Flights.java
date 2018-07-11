package com.trainologic.spark.course.exercises;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Flights {
    public static void airportsBase() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/airports.csv");

        revenueDF.printSchema();

        revenueDF.filter("Country = 'Greenland'").show();

        revenueDF.filter(
                (FilterFunction<Row>) value ->
                        value.getString(3).toLowerCase().startsWith("a") &&
                                value.getString(1).split(" ").length < 3
        ).show();

        Dataset<Row> wcDF = revenueDF.withColumn(
                "airport_name_legth", length(revenueDF.col("name")));
        wcDF.show();
        wcDF.filter("airport_name_legth > 4").show();
    }

    public static void airportsAgg() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> airportsDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/airports.csv");

        airportsDF.dropDuplicates("country").show();
        System.out.println(airportsDF.select("country").distinct().count());

        airportsDF.groupBy("country").count().show();

        Dataset<Row> countryCountDF = airportsDF
                .groupBy("country").count()
                .filter("count > 20")
                .orderBy(desc("count"));

        countryCountDF.show();

    }

    public static void airportsSQL() {
        SparkSession spark = SparkUtils.createSparkSession();
        spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/airports.csv")
                .createOrReplaceTempView("airports");

        spark.sql("select count(distinct country) as country_count " +
                "from airports").show();

        Dataset<Row> df = spark.sql("select country, count(*) as ap_count " +
                "from airports " +
                "group by country " +
                "order by ap_count desc ");
        df.createOrReplaceTempView("new_ap");

        spark.sql("select * from new_ap").show();

        spark.sql("show tables").show();


    }

    public static void flightsDF() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> airportsDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/airports.csv");

        Dataset<Row> flightsDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/flights.csv");

//        Dataset<Row> apUpdated = airportsDF.select("country", "code");
//        Dataset<Row> withCity2 = flightsDF
//                .join(apUpdated, flightsDF.col("code").equalTo(apUpdated.col("origin")));

        Dataset<Row> withCity = flightsDF
                .withColumn("code", col("origin"))
                .join(airportsDF.select("country", "code"), "code");
        withCity.show();

        Dataset<Row> ko = withCity.select("year", "Month", "DayofMonth", "DepTime", "code", "origin", "country", "DayofMonth", "UniqueCarrier");
        ko.groupBy("DayofMonth", "country").count().show();

        ko
                .filter("year='2008' and Month='1'")
                .filter(col("DayofMonth").equalTo(3))
                .withColumn("hour", col("DepTime").substr(0, 2))
                .groupBy("country", "hour").count().orderBy(col("count").desc())
                .show();

    }

    public static void main(String[] args) {
//        airportsBase();
//        airportsAgg();
//        flightsDF();
        airportsSQL();
    }
}
