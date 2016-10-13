package com.test.dpp;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by suneel on 08/10/2016.
 */
public class EnronSparkProcessor {

    public JavaSparkContext sparkContext;
    public EnronSparkProcessor(String path){
       sparkContext = initializeSpark();
       JavaPairRDD<Text,BytesWritable> zipFileRDD = sparkContext.newAPIHadoopFile(path,new ZipFileInputFormat().getClass(),Text.class,
               BytesWritable.class,sparkContext.hadoopConfiguration());



    }

    public JavaSparkContext initializeSpark() {
        final SparkConf sparkConf = new SparkConf().setAppName("EnronEmailProcessor");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        return ctx;
    }
    public static void main(String args[]) {
        String path = args[0];
       new EnronSparkProcessor(path);
    }


}
