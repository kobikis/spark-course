package com.trainologic.spark.course.precourse;

import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;


public class PresonDS {
    public static class Person implements Serializable {
        private String name;
        private int age;

        public Person(String name, int age){
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkUtils.createSparkSession();

        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        Dataset<Person> personDS = spark.createDataset(
                Arrays.asList(
                        new Person("Ophir", 44),
                        new Person("Yossi", 32),
                        new Person("Yoav", 58)

                ),
                personEncoder
        );

        personDS.show();
        personDS.filter("age > 40").show();
    }
}
