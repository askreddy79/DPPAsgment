package com.test.dpp;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by suneel on 08/10/2016.
 */
public class EnronSparkProcessor {

    public static void main(String args[]) {
        String sourceFolder = "hdfs://";



        final SparkConf sparkConf = new SparkConf().setAppName("EnronEmailProcessor");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);


    }
}
