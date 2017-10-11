package com.shixzh.spark.learning.file_5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

public class CassandraTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf(true).set("spark.cassandra.connect.host", "hostname");
        JavaSparkContext sc = new JavaSparkContext("local", "basicquerycassandra", conf);
        JavaRDD<CassandraRow> data = CassandraJavaUtil.javaFunctions(sc).cassandraTable("test", "kv");
        System.out.println(data.mapToDouble(new DoubleFunction<CassandraRow>() {

            private static final long serialVersionUID = 1L;

            public double call(CassandraRow row) throws Exception {
                return row.getInt("value");
            }
        }));
    }
}
