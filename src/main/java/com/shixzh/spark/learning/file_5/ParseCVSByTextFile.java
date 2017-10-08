package com.shixzh.spark.learning.file_5;

import java.io.StringReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.shixzh.spark.learning.util.ResourceManager;

import au.com.bytecode.opencsv.CSVReader;

public class ParseCVSByTextFile implements Function<String, String[]> {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
				.setSparkHome(ResourceManager.getSparkHome());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> input = sc.textFile(ResourceManager.getResourceFilePath() + "person.csv");
		JavaRDD<String[]> result = input.map(new ParseCVSByTextFile());
		// input.saveAsTextFile(ResourceManager.getResultPath());
		String[] first = result.first();
		for (String f : first) {
			System.out.println(f);
		}
	}

	public String[] call(String t) throws Exception {
		CSVReader reader = new CSVReader(new StringReader(t));
		return reader.readNext();
	}
}
