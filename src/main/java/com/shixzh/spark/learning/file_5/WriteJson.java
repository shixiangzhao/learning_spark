package com.shixzh.spark.learning.file_5;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shixzh.spark.learning.util.ResourceManager;

public class WriteJson implements FlatMapFunction<Iterator<Person>, String> {

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
                .setSparkHome(ResourceManager.getSparkHome());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sc.textFile(ResourceManager.getResourceFilePath() + "person.json");
        JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(new LikesPandas());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        formatted.saveAsTextFile(ResourceManager.getResultPath());
    }

    public Iterator<String> call(Iterator<Person> people) throws Exception {
        ArrayList<String> text = new ArrayList<String>();
        ObjectMapper om = new ObjectMapper();
        while (people.hasNext()) {
            Person person = people.next();
            try {
                text.add(om.writeValueAsString(person));
            } catch (Exception e) {
                System.out.println("Error: data format is wrong!");
            }
        }
        return text.iterator();
    }
}
