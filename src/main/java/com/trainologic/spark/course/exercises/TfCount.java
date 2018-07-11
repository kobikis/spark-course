package com.trainologic.spark.course.exercises;

public class TfCount {
        private String document;
        private String word;
        private double tf;

    public TfCount(String document, String word, double tf) {
        this.document = document;
        this.word = word;
        this.tf = tf;
    }

    public TfCount() {
    }

    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public double getTf() {
        return tf;
    }

    public void setTf(double tf) {
        this.tf = tf;
    }
}
