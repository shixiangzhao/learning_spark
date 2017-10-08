package com.shixzh.spark.learning.pairrdd_4;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class TestPairFunction {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My First Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1", "3", "6"));
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String t) throws Exception {
				return new Tuple2<String, String>(t.split(" ")[0], t);
			}
		};
		JavaPairRDD<String, String> pairs = rdd.mapToPair(keyData);
		printPairRDD(pairs);

		ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
		Tuple2<String, String> tuple2 = new Tuple2<String, String>("i", "love");
		list.add(tuple2);
		JavaPairRDD<String, String> pairs2 = sc.parallelizePairs(list);
		printPairRDD(pairs2);
	}

	private static void printPairRDD(JavaPairRDD<String, String> pairs) {
		pairs.persist(StorageLevel.DISK_ONLY());
		System.out.println(pairs.count());
		System.out.println(pairs.take(1));
	}

}
