package com.trainologic.spark.course.sparksql;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;


public class SparkSQL {


    public static void simpleSQL() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark.read().option("header", true).csv("src/main/resources/sparksql/revenues.csv");

        revenueDF.createOrReplaceTempView("rev");

        spark.sql("show tables").show();

        //Full
        Dataset<Row> fullDF = spark.sql("select * from rev");

        //Sum by category
        Dataset<Row> sumDF = spark
                .sql(
                        "select category, sum(revenue) as rev_sum " +
                                "from rev " +
                                "group by category");
        sumDF.show();

        sumDF.printSchema();
    }

    public static void UDFs() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/revenues.csv")
                .withColumn("revenue", col("revenue").cast(IntegerType));
        revenueDF.createOrReplaceTempView("rev");


        UDF1<Integer, Double> square = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer integer) throws Exception {
                return Math.sqrt(integer);
            }
        };

        spark.udf().register("sqr", square, DataTypes.DoubleType);

        //Sum by category
        Dataset<Row> sumDF = spark
                .sql(
                        "select category, sqr(revenue) as rev_sum " +
                                "from rev ");
        sumDF.show();

        revenueDF.select(callUDF("sqr", col("revenue")).as("sqrt")).show();


    }


    public static void main(String[] args) {

        simpleSQL();
//        UDFs();
    }
}
