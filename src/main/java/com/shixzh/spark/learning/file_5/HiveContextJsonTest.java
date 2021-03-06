package com.shixzh.spark.learning.file_5;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.shixzh.spark.learning.util.ResourceManager;

public class HiveContextJsonTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Hive Context Test")
                .setSparkHome(ResourceManager.getSparkHome());
        SparkContext sc = new SparkContext(sparkConf);
        HiveContext hiveCtx = new HiveContext(sc);
/*
        SchemaRDD rows = hiveCtx.jsonFile(jsonFile);
        rows.registerAsTable("tweets");
        SchemaRDD results = hiveCtx.sql("SELECT user.name, text FROM tweets");
        Row firstRow = results.first();
        System.out.println(firstRow.getString(0));
*/
    }
}
