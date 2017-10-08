package com.shixzh.spark.learning.file_5;

import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.shixzh.spark.learning.util.ResourceManager;

import scala.Tuple2;

public class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
				.setSparkHome(ResourceManager.getSparkHome());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<Tuple2<String, Integer>> input = null;
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
		JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
		result.saveAsHadoopFile(ResourceManager.getResultPath(), Text.class, IntWritable.class,
				SequenceFileOutputFormat.class);
	}

	public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) throws Exception {
		return new Tuple2(new Text(record._1), new IntWritable(record._2));
	}
}
