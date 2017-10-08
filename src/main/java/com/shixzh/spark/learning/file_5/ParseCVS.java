package com.shixzh.spark.learning.file_5;

import java.io.StringReader;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.shixzh.spark.learning.util.ResourceManager;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class ParseCVS implements FlatMapFunction<Tuple2<String, String>, String[]> {

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
                .setSparkHome(ResourceManager.getSparkHome());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> input = sc.wholeTextFiles(ResourceManager.getResourceFilePath() + "person.json");
        JavaRDD<String[]> result = input.flatMap(new ParseCVS());
    }

	public Iterator<String[]> call(Tuple2<String, String> t) throws Exception {
		CSVReader reader = new CSVReader(new StringReader(t._2));
		return reader.readAll().iterator();
	}
}
