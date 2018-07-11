package com.trainologic.spark.course.sparksql;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.IntegerType;


public class BasicDFs {

    public static void simpleDF() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark.read().option("header", true).csv("src/main/resources/sparksql/revenues.csv");
        revenueDF.show();

        //What are the columns?
        revenueDF.printSchema();

        //Only revenues that are more than 3000:
        revenueDF.filter("revenue > 3000").show();

        //Only tablets'
        revenueDF.filter("category = 'Tablet'").show();
        //In Scala its much more cool!
        revenueDF.filter(col("category").equalTo("Tablet")).show();

        revenueDF.filter(
                (FilterFunction<Row>) value ->
                        value.getString(1).equals("Tablet")).show();
//        //See only products
        revenueDF.select("product").show();
//
//        //See only products and each only once
        revenueDF.select("product").distinct().show();
//
//        //How many different revenues??
        System.out.println("Count ---> " + revenueDF.select("revenue").distinct().count());
        spark.close();

    }

    public static void colsDF() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark.read().option("header", true).csv("src/main/resources/sparksql/revenues.csv");

        //drop col
        revenueDF.drop("category").show();

        //Adding static col
        Dataset<Row> wcDF = revenueDF
                .withColumn("ones", lit(1))
                .withColumn("twos", lit("two!"));


        wcDF.printSchema();
        wcDF.show();

//        revenueDF.withColumn("more_money", revenueDF.col("revenue").plus(3000)).show();
    }

    public static void aliases() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark.read().option("header", true).csv("src/main/resources/sparksql/revenues.csv");

        revenueDF.as("asda").printSchema();

        revenueDF.withColumnRenamed("revenue", "revenue2").printSchema();


    }


        public static void simplePlusDF() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark.read().option("header", true).csv("src/main/resources/sparksql/revenues.csv");

        //Lets give everybody raise:
        revenueDF.select(col("product"), col("category"), col("revenue"), col("revenue").plus(1000).as("rev++")).show();
        revenueDF.select(col("product"), col("category"), col("revenue"), col("revenue").plus(1000)).printSchema();


        //Lets make col with int type
        Dataset<Row> wColDF = revenueDF
                .withColumn(
                        "int_revenue",
                        col("revenue").cast(IntegerType));

        wColDF.printSchema();
        wColDF.show();
    }

    public static void createDF() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF =
                spark
                        .read()
                        .option("header", true)
                        .csv("src/main/resources/sparksql/revenues.csv");
        Dataset<Row> updatedSchemaDF = spark.createDataFrame(revenueDF.rdd(), Revenue.class);

        updatedSchemaDF.printSchema();

        updatedSchemaDF.show();

        updatedSchemaDF.filter((FilterFunction<Row>)row -> row.getInt(2) > 300).show();

    }

    public static void aggregations() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Row> revenueDF = spark.read().option("header", true).csv("src/main/resources/sparksql/revenues.csv");
        Dataset<Row> wColDF = revenueDF.withColumn("revenue", revenueDF.col("revenue").cast(IntegerType));

        revenueDF.groupBy("category").count().show();

        //If not only one metrics:
        RelationalGroupedDataset rgs = wColDF.groupBy("category");

        rgs.sum("revenue").show();
        rgs.avg("revenue").show();
        rgs.min("revenue").show();

    }


    public static void main(String[] args) {
//        simpleDF();
//        colsDF();
//        simplePlusDF();
//        aliases();
        aggregations();
    }
}
