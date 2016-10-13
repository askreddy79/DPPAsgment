package com.test.dpp;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Suneel on 10/13/2016.
 */
public class ZipFileInputFormat extends FileInputFormat<Text,BytesWritable> {

    @Override
    /**
     * We tell hadoop not to split the zip file.
     */
    protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext ctx, Path filename){
        return false;
    }


    @Override
    public RecordReader<Text,BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException{
        return new ZipFileRecordReader();
    }
}
