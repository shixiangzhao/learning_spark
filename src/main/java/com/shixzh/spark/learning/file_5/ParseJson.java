package com.shixzh.spark.learning.file_5;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shixzh.spark.learning.util.ResourceManager;

public class ParseJson implements FlatMapFunction<Iterator<String>, Person> {

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
                .setSparkHome(ResourceManager.getSparkHome());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sc.textFile(ResourceManager.getResourceFilePath() + "person.json");
        JavaRDD<Person> result = input.mapPartitions(new ParseJson());
    }

    public Iterator<Person> call(Iterator<String> lines) throws Exception {
        ArrayList<Person> people = new ArrayList<Person>();
        ObjectMapper om = new ObjectMapper();
        while (lines.hasNext()) {
            String line = lines.next();
            try {
                people.add(om.readValue(line, Person.class));
            } catch (Exception e) {
                System.out.println("Error data format!");
            }
        }
        return people.iterator();
    }
}
