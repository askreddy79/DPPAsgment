package com.test.dpp;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by suneel on 08/10/2016.
 */
public class EnronSparkProcessor {
    static Logger LOG = Logger.getLogger(EnronSparkProcessor.class);
    public JavaSparkContext sparkContext;
    public EnronSparkProcessor(String path) throws IOException{
       sparkContext = initializeSpark();
       Job job = Job.getInstance(sparkContext.hadoopConfiguration());
       JavaPairRDD<Text,BytesWritable> zipFileRDD = sparkContext.newAPIHadoopFile(path,new ZipFileInputFormat().getClass(),Text.class,
               BytesWritable.class,job.getConfiguration());


    }

    public JavaSparkContext initializeSpark() {
        final SparkConf sparkConf = new SparkConf().setAppName("EnronEmailProcessor");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        return ctx;
    }
    public static void main(String args[]) {
        String path = args[0];
        try {
            new EnronSparkProcessor(path);
        } catch(IOException ex) {
            LOG.info("Error in main method while processing EnronSpark: " +  ex.getMessage());
        }

    }


}
