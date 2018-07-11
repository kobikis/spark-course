package com.trainologic.spark.course.sparksql;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;



public class Datasets {
    public static void primitiveDS() {
        SparkSession spark = SparkUtils.createSparkSession();
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);


        primitiveDS.show();

        Dataset<String> transformedDS = primitiveDS.map(
                (MapFunction<Integer, String>) value -> "I'm a String!: " + value.toString(),
                Encoders.STRING());

        transformedDS.show();
    }

    public static void typedDS() {
        SparkSession spark = SparkUtils.createSparkSession();
        Dataset<Integer> intDS =
                spark.createDataset(Arrays.asList(1, 2, 3, 4, 5), Encoders.INT());
        PU.show("Filtered DS", intDS.filter(
                (FilterFunction<Integer>) i -> i > 2)
        );

        StructType schema = DataTypes
                .createStructType(
                        new StructField[]
                                {DataTypes.createStructField("value", DataTypes.IntegerType, false)});


        Dataset<Row> intDF = spark
                .createDataFrame(
                        Arrays.asList(
                                RowFactory.create(1),
                                RowFactory.create(2),
                                RowFactory.create(3),
                                RowFactory.create(4),
                                RowFactory.create(5)),
                        schema);

        //This will compile and work
        PU.show("Filtered DF", intDF.filter((FilterFunction<Row>) row -> row.getInt(0) > 3));

        //This will compile but will fail on runtime!
        PU.show("Wont work DF", intDF.filter((FilterFunction<Row>) row -> row.getInt(2) > 3));

        //This will compile but will fail on runtime!
        PU.show("Wont work DF", intDF.filter((FilterFunction<Row>) row -> row.getString(0).length() > 3));

        //This won't compile
//        PU.show("Wont work DF", intDS.filter((FilterFunction<Integer>) i -> i.getString(0).length() > 3));
        Dataset<Integer> res = intDS.map((MapFunction<Integer, Integer>) i -> i + i, Encoders.INT());
    }


    public static void classDS() {
        SparkSession spark = SparkUtils.createSparkSession();

        List<Transaction> tnsList = Arrays.asList(
                new Transaction("Ophir", "home", 200l),
                new Transaction("Yossi", "home", 100l),
                new Transaction("Yossi", "car", 500l),
                new Transaction("Ophir", "car", 200l),
                new Transaction("Ophir", "car", 100l)
        );

        Encoder<Transaction> tnsEncoder = Encoders.bean(Transaction.class);

        Dataset<Transaction> trnsDS = spark.createDataset(tnsList, tnsEncoder);

        trnsDS.show();


       trnsDS.map(
                (MapFunction<Transaction, Transaction>)
                        tns -> new Transaction(tns.getName(), tns.getCategory(), tns.getAmount() * 2),
                tnsEncoder).show();

        trnsDS.map(
                (MapFunction<Transaction, String>) tns -> tns.getName(),
                Encoders.STRING()).show();

       trnsDS.map(
                (MapFunction<Transaction, String>) Transaction::getName,
                Encoders.STRING())
                .distinct().show();

        trnsDS.groupBy("category").sum("amount").show();
    }

    public static void fileDS() {
        SparkSession spark = SparkUtils.createSparkSession();
        Encoder<Revenue> revEncoder = Encoders.bean(Revenue.class);

        Dataset<Revenue> revenueDataset =
                spark
                        .read()
                        .option("header", true)
                        .csv("scala-course/src/main/resources/revenues.csv").as(revEncoder);

        PU.show("Revenues: ", revenueDataset);

        Dataset<Row> df = spark
                        .read()
                        .option("header", true)
                        .csv("scala-course/src/main/resources/revenues.csv");

        Dataset<String> with100 = revenueDataset
                .map((MapFunction<Revenue, String>) Revenue::getCategory, Encoders.STRING());


        PU.show("Add 1000 to cell phone category: ", with100);
    }

    public static void main(String[] args) {
//        primitiveDS();
        classDS();
//        typedDS();
//        fileDS();
    }
}

