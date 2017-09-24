package com.shixzh.spark.learning.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

public class TestJavaDoubleRDD {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My First Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 3, 6));
		JavaDoubleRDD result = rdd.mapToDouble(new DoubleFunction<Integer>() {
			public double call(Integer x) {
				return (double) x * x;
			}
		});
		System.out.println(result.mean());
	}
}
