package com.shixzh.spark.learning.util;

public class ResourceManager {

    public static final String JCAT_LOG = "jcatlog.txt";
    public static final String RESULT_PATH = "result";
    public static final String USER_PATH = System.getProperty("user.dir");
    public static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String SPARK_HOME = System.getenv("SPARK_HOME");

    public static String getJcatLogPath() {
        return USER_PATH + FILE_SEPARATOR + JCAT_LOG;
    }

    public static String getResultPath() {
        return USER_PATH + FILE_SEPARATOR + RESULT_PATH;
    }

    public static String getSparkHome() {
        return SPARK_HOME;
    }
}
