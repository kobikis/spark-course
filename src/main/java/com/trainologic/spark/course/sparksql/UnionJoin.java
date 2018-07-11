package com.trainologic.spark.course.sparksql;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;


public class UnionJoin {

    public static void unionDF() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/sparksql/revenues.csv")
                .withColumn("revenue", col("revenue").cast(IntegerType));
        revenueDF.show();

        revenueDF.groupBy("category").sum("revenue").sort("category").show();
        revenueDF.union(revenueDF).groupBy("category").sum("revenue").sort("category").show();

        revenueDF.join(revenueDF, "product").show();
        spark.close();

    }


    public static void main(String[] args) {
        unionDF();
    }
}
