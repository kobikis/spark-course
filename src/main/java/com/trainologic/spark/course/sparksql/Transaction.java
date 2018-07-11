package com.trainologic.spark.course.sparksql;

import java.io.Serializable;

public class Transaction implements Serializable {
    private String name;
    private String category;
    private Long amount;


    public String getCategory() {
        return category;
    }

    public Transaction(String name, String category, Long amount) {
        this.name = name;
        this.amount = amount;
        this.category = category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Transaction() {
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }
}
