package com.shixzh.spark.learning.rdd_3;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.storage.StorageLevel;

public class TestPersist {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My First Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 3, 6));
		JavaDoubleRDD result = rdd.mapToDouble(new DoubleFunction<Integer>() {
			public double call(Integer x) {
				return (double) x * x;
			}
		});
		result.persist(StorageLevel.DISK_ONLY());
		System.out.println(result.count());
		System.out.println(result.take(1));
	}
}
