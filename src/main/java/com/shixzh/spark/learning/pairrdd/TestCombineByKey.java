package com.shixzh.spark.learning.pairrdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.shixzh.spark.learning.util.ResourceManager;

import scala.Tuple2;

public class TestCombineByKey {

	public static class AvgCount implements Serializable {
		public int total_;
		public int num_;

		public AvgCount(int total, int num) {
			total_ = total;
			num_ = num;
		}

		public float avg() {
			return total_ / (float) num_;
		}
	}

	public static void main(String[] args) {
		Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {

			public AvgCount call(Integer v1) throws Exception {
				return new AvgCount(v1, 1);
			}
		};

		Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {

			public AvgCount call(AvgCount v1, Integer v2) throws Exception {
				v1.total_ += v2;
				v1.num_ += 1;
				return v1;
			}
		};

		Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {

			public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
				v1.total_ += v2.total_;
				v1.num_ += v2.num_;
				return v1;
			}
		};

		AvgCount initial = new AvgCount(0, 0);
		JavaPairRDD<String, Integer> counts = getWordCount();
		JavaPairRDD<String, AvgCount> avgCounts = counts.combineByKey(createAcc, addAndCount, combine);
		Map<String, AvgCount> countMap = avgCounts.collectAsMap();
		for (Entry<String, AvgCount> entry : countMap.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue().avg());
		}
	}

	public static JavaPairRDD<String, Integer> getWordCount() {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count")
				.setSparkHome(ResourceManager.getSparkHome());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> input = sc.textFile(ResourceManager.getJcatLogPath());
		JavaPairRDD<String, Integer> counts = input.flatMap(new FlatMapFunction<String, String>() {

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
		return counts;
	}

}
