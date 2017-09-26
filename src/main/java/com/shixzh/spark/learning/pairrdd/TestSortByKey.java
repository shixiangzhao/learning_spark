package com.shixzh.spark.learning.pairrdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.shixzh.spark.learning.util.ResourceManager;

import scala.Tuple2;

public class TestSortByKey {

	public static void main(String[] args) {
		JavaPairRDD<Integer, String> rdd = getJavaPairRDD();
		IntegerComparator comp = new IntegerComparator();
		JavaPairRDD<Integer, String> rddSort = rdd.sortByKey(comp);
		Iterator<Tuple2<Integer, String>> iterator = rddSort.take(4).iterator();
		while (iterator.hasNext()) {
			System.out.println(iterator.next());

		}
	}

	public static JavaPairRDD<Integer, String> getJavaPairRDD() {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sort by key")
				.setSparkHome(ResourceManager.getSparkHome());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> input = sc.parallelize(Arrays.asList("panda", "monkey", "tiger", "panda"));
		JavaPairRDD<Integer, String> counts = input.mapToPair(new PairFunction<String, Integer, String>() {
			Random random = new Random(49);

			public Tuple2<Integer, String> call(String t) throws Exception {
				int v = random.nextInt(20);
				System.out.println(t + ": " + v);
				return new Tuple2<Integer, String>(v, t);
			}
		});
		return counts;
	}
}

class IntegerComparator implements Comparator<Integer>, Serializable {

	private static final long serialVersionUID = 1L;

	public int compare(Integer a, Integer b) {
		return String.valueOf(a).compareTo(String.valueOf(b));
	}
}
