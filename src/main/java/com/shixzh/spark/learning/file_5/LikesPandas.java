package com.shixzh.spark.learning.file_5;

import org.apache.spark.api.java.function.Function;

public class LikesPandas implements Function<Person, Boolean> {

    private static final long serialVersionUID = 1L;

    public Boolean call(Person v1) throws Exception {
        return v1.getCountry().equals("China");
    }
}
