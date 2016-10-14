package com.test.dpp;

import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

/**
 * Created by suneel on 08/10/2016.
 */
public class EnronSparkProcessor implements Serializable {
    static Logger LOG = Logger.getLogger(EnronSparkProcessor.class);
    static int depth = -1;

    public static JavaSparkContext sparkContext;
//    public EnronSparkProcessor(String path) throws IOException{
//       sparkContext = initializeSpark();
//       Job job = Job.getInstance(sparkContext.hadoopConfiguration());
//       JavaPairRDD<Text,BytesWritable> zipFileRDD = sparkContext.newAPIHadoopFile(path,new ZipFileInputFormat().getClass(),Text.class,
//               BytesWritable.class,job.getConfiguration());
//
//      JavaPairRDD<String,Integer> rdd =   zipFileRDD.mapToPair(zipFileRec -> {
//            LOG.info("file name" + zipFileRec._1());
//            PSTFile pstFile = new PSTFile(zipFileRec._2().getBytes());
//            int noOfwords = processFolder(pstFile.getRootFolder());
//            LOG.info("noOfwords-->" + noOfwords);
//            return new Tuple2<String, Integer>(zipFileRec._1().getBytes().toString(),noOfwords);
//        });
//
//        rdd.foreach(rec -> {
//            LOG.info("Zip file name : " + rec._1() + " --- Zip file wordcount : " + rec._2().intValue() );
//        });
//
//    }

    public void printDepth() {
        for (int x=0;x<depth-1;x++){
            LOG.info(" | ");
        }
        LOG.info(" |- ");
    }


    public static int  processFolder(PSTFolder folder) throws PSTException, IOException {
        int pstCount = 0;
        int countWords = 0;
        depth++;

        if (depth > 0) {
           // printDepth();
        }


        if (folder.hasSubfolders()) {
            Vector<PSTFolder> childFolders = folder.getSubFolders();
            for (PSTFolder childFolder:childFolders) {
                processFolder(childFolder);
            }
        }

        if (folder.getContentCount() > 0) {
            depth++;
            PSTMessage email = (PSTMessage) folder.getNextChild();

            while(email!=null){
                //printDepth();
                LOG.info("Email: " +email.getSubject());
                //LOG.info("EMail body:" + email.getBody());
                countWords = countWords + countWords(email.getBody());
                email = (PSTMessage) folder.getNextChild();
                pstCount++;

            }
            depth--;
        }
        depth--;
        return countWords/pstCount;
    }

    public static int countWords(String s){

        int wordCount = 0;

        boolean word = false;
        int endOfLine = s.length() - 1;

        for (int i = 0; i < s.length(); i++) {
            // if the char is a letter, word = true.
            if (Character.isLetter(s.charAt(i)) && i != endOfLine) {
                word = true;
                // if char isn't a letter and there have been letters before,
                // counter goes up.
            } else if (!Character.isLetter(s.charAt(i)) && word) {
                wordCount++;
                word = false;
                // last word of String; if it doesn't end with a non letter, it
                // wouldn't count without this.
            } else if (Character.isLetter(s.charAt(i)) && i == endOfLine) {
                wordCount++;
            }
        }
        return wordCount;
    }

    public static JavaSparkContext initializeSpark() {
        final SparkConf sparkConf = new SparkConf().setAppName("EnronEmailProcessor");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        return ctx;
    }
    public static void main(String args[]) {
        String path = args[0];
        try {
            //new EnronSparkProcessor(path);

            sparkContext = initializeSpark();
            Job job = Job.getInstance(sparkContext.hadoopConfiguration());
            JavaPairRDD<Text,BytesWritable> zipFileRDD = sparkContext.newAPIHadoopFile(path,new ZipFileInputFormat().getClass(),Text.class,
                    BytesWritable.class,job.getConfiguration());

            JavaPairRDD<String,Integer> rdd =   zipFileRDD.mapToPair(zipFileRec -> {
                LOG.info("file name" + zipFileRec._1());
                PSTFile pstFile = new PSTFile(zipFileRec._2().getBytes());
                int noOfwords = processFolder(pstFile.getRootFolder());
                LOG.info("noOfwords-->" + noOfwords);
                return new Tuple2<String, Integer>(zipFileRec._1().getBytes().toString(),noOfwords);
            });

            rdd.foreach(rec -> {
                LOG.info("Zip file name : " + rec._1() + " --- Zip file wordcount : " + rec._2().intValue() );
            });

        } catch(IOException ex) {
            LOG.info("Error in main method while processing EnronSpark: " +  ex.getMessage());
        }

    }


}
