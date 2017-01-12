package com.fang.spark;

/**
 * Created by fang on 17-1-12.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.opencv.core.Mat;

import java.io.IOException;
public class ImageInputFormat extends FileInputFormat<Text,Mat> {

    @Override
    protected boolean isSplitable(JobContext context, Path path){
        return false;//保证单个图不被分割
    }

    @Override
    public RecordReader<Text, Mat> createRecordReader(InputSplit arg0,
                                                                TaskAttemptContext arg1) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return new ImageRecordReader();
    }
}