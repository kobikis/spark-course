package com.trainologic.spark.course.exercises;

import java.io.Serializable;
import java.sql.Timestamp;

public class Stock implements Serializable {
    private String name;
    private Integer price;
    private Timestamp ts;



    public Stock() {
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Stock(String name, Integer price, Timestamp ts) {

        this.name = name;
        this.price = price;
        this.ts = ts;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }
}
