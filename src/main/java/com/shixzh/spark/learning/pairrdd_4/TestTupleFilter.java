package com.shixzh.spark.learning.pairrdd_4;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class TestTupleFilter {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My First Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
		Tuple2<String, String> tuple2 = new Tuple2<String, String>("hodlen", "likes coffee");
		Tuple2<String, String> tuple3 = new Tuple2<String, String>("panda", "likes long strings and coffee");
		list.add(tuple2);
		list.add(tuple3);
		JavaPairRDD<String, String> pairs = sc.parallelizePairs(list);
		
		Function<Tuple2<String, String>, Boolean> longWordFilter = 
				new Function<Tuple2<String,String>, Boolean>() {
					public Boolean call(Tuple2<String, String> keyValue) {
						return (keyValue._2.length() < 20);
					}
				};
		JavaPairRDD<String, String> result = pairs.filter(longWordFilter);
		printPairRDD(result);
	}

	private static void printPairRDD(JavaPairRDD<String, String> pairs) {
		pairs.persist(StorageLevel.DISK_ONLY());
		System.out.println(pairs.count());
		System.out.println(pairs.take(1));
	}

}
