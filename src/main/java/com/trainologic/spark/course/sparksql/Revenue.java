package com.trainologic.spark.course.sparksql;

import java.io.Serializable;

public class Revenue implements Serializable {
    private String product;
    private String category;
    private Integer revenue;

    public Revenue(){
    }

    public Revenue(String product, String category, Integer revenue) {
        this.product = product;
        this.category = category;
        this.revenue = revenue;
    }

    public Revenue(String product, String category, String revenue) {
        this(product, category, Integer.valueOf(revenue));
    }


    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Integer getRevenue() {
        return revenue;
    }

    public void setRevenue(Integer revenue) {
        this.revenue = revenue;
    }
}
