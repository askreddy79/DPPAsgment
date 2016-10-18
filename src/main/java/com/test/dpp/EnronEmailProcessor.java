package com.test.dpp;

import com.pff.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.ListBuffer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * Created by suneel on 18/10/2016.
 */
public class EnronEmailProcessor implements Serializable {
    public static JavaSparkContext sparkContext;
    private static Logger LOG = Logger.getLogger(EnronSparkProcessor.class);
    static int depth = -1;
    public static void main(String args[]) {
        String path = args[0];
        sparkContext = initializeSpark();
        try {
            Job job = Job.getInstance(sparkContext.hadoopConfiguration());
            JavaPairRDD<Text,BytesWritable> zipFileRDD = sparkContext.newAPIHadoopFile(path,ZipFileInputFormat.class,Text.class,
                    BytesWritable.class,job.getConfiguration());

            JavaPairRDD<String,PSTFile> enronEmailRDD = zipFileRDD.mapToPair(zipFileRec -> {
                return new Tuple2<String,PSTFile>(zipFileRec._1.toString(),new PSTFile(zipFileRec._2.getBytes()));
            });

            //for word counts
            enronEmailRDD.map(pstFileRec-> {
                return processEmails(pstFileRec._1,pstFileRec._2);
            }).saveAsTextFile("/user/hadoop/enron/result1.txt");

            JavaRDD<EnronEmail> enronEmailJavaRDD =  enronEmailRDD.flatMap(pstFileRec->{
                return processReceipts(pstFileRec._1,pstFileRec._2);
            });

           JavaRDD<List<Tuple2<String,Double>>> enronRecs = enronEmailJavaRDD.map(rec -> {
                List<Tuple2<String,Double>> toAndCCList = new ArrayList<Tuple2<String, Double>>();
                for(String to : rec.getToList()) {
                    toAndCCList.add(new Tuple2<String,Double>(to,Double.valueOf(1.0)));
                }
                for(String cc : rec.getCcList()) {
                    toAndCCList.add(new Tuple2<String,Double>(cc,Double.valueOf(0.5)));
                }
                return toAndCCList;
            });

            enronRecs.flatMap(listRec -> listRec).mapToPair(rec -> rec)
                    .reduceByKey((a,b)->a+b).sortByKey().saveAsTextFile("/user/hadoop/enron/result1.txt");

        }
        catch(IOException ex) {
         LOG.info("Error in main method while processing EnronSpark: " +  ex.getMessage());
        }



    }

    public static List<EnronEmail> processReceipts(String zipFileName,PSTFile pstFile) throws IOException,PSTException{
        List<EnronEmail> enronEmailList = new ArrayList<EnronEmail>();
        processFolder(zipFileName,pstFile.getMessageStore().getDisplayName(),
                pstFile.getRootFolder(),0,enronEmailList);
        return enronEmailList;
    }



    public static Tuple3<String,Long,Double> processEmails(String fileName,PSTFile pstFile)
            throws IOException,PSTException{
        List<EnronEmail> enronEmailList = new ArrayList<EnronEmail>();
        processFolder(fileName,pstFile.getMessageStore().getDisplayName(),
                pstFile.getRootFolder(),0,enronEmailList);
        long wordCount = 0;
        for(EnronEmail enronEmail:enronEmailList){
            wordCount += countWords(enronEmail.getEmail());
        }
        Double average = enronEmailList.size()>0 ? Double.valueOf(wordCount/enronEmailList.size()) : 0;
        return new Tuple3<String,Long,Double> (fileName,Long.valueOf(wordCount), average);
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

    public static int processFolder(String zipFileName, String pstFileName,PSTFolder folder,
                                    int depth, List<EnronEmail> emailList) throws IOException,PSTException{
        int folderTotal = 0;
        if (depth > 0) {
           int contentCount = folder.getContentCount();
           LOG.info("contentCount: " + contentCount);
           folderTotal += contentCount;
           if (contentCount > 0) {
               processFolderContent(zipFileName,folder,depth+1,emailList);
           }

        }

        if (folder.hasSubfolders()){
            Vector<PSTFolder> childPstFolders = folder.getSubFolders();
            for (PSTFolder pstFolder : childPstFolders) {
                folderTotal += processFolder(zipFileName,pstFileName,pstFolder,depth+1,emailList);
            }
        }


        return folderTotal;
    }


    public static void processFolderContent(String zipFileName,PSTFolder folder,
                                            int depth,List<EnronEmail> emailList) throws IOException,PSTException{
        PSTMessage pstMessage = (PSTMessage)folder.getNextChild();
       while (pstMessage != null){
           processEmail(zipFileName,folder.getDisplayName(),pstMessage,depth,emailList);
           pstMessage = (PSTMessage)folder.getNextChild();
       }
    }

    public static void processEmail(String zipFileName,String folder,
                                    PSTMessage email,int depth,List<EnronEmail> emailList) {
       emailList.add(new EnronEmail(zipFileName, folder,
                Arrays.asList(email.getDisplayTo().split(";")),
                Arrays.asList(email.getDisplayCC().split(";")),
                email.getSubject(),email.getBody()));
    }

    public static JavaSparkContext initializeSpark() {
        final SparkConf sparkConf = new SparkConf().setAppName("EnronEmailProcessor");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        return ctx;
    }


}


