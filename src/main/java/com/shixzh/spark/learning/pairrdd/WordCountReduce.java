package com.shixzh.spark.learning.pairrdd;

import java.util.Arrays;
import java.util.Iterator;
import java.lang.Iterable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.shixzh.spark.learning.util.ResourceManager;

import scala.Tuple2;

public class WordCountReduce {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Word Count")
                .setSparkHome(ResourceManager.getSparkHome());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sc.textFile(ResourceManager.getJcatLogPath());
        JavaPairRDD<String, Integer> counts = input.flatMap(
                new FlatMapFunction<String, String>() {

                    public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }
                }).mapToPair(new PairFunction<String, String, Integer>() {

                    public Tuple2<String, Integer> call(String t) throws Exception {
                        return new Tuple2<String, Integer>(t, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        counts.saveAsTextFile(ResourceManager.getResultPath());
    }
}
