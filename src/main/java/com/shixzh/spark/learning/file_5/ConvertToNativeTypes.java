package com.shixzh.spark.learning.file_5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.shixzh.spark.learning.util.ResourceManager;

import scala.Tuple2;

public class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
				.setSparkHome(ResourceManager.getSparkHome());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(ResourceManager.getResourceFilePath() + "person.json",
				Text.class, IntWritable.class);
		JavaPairRDD<String, Integer> result = input.mapToPair(new ConvertToNativeTypes());
	}

	public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) throws Exception {
		return new Tuple2(record._1.toString(), record._2.get());
	}
}
