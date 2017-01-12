package com.fang.spark;

/**
 * Created by fang on 17-1-12.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;

import java.io.IOException;

public class ImageRecordReader extends RecordReader<Text, Mat> {

    private Text key=null;
    private Mat value=null;
    private FSDataInputStream fileStream=null;
    private FileSplit filesplit;
    private boolean processed= false;
    private Configuration conf;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        filesplit = (FileSplit)split;
        conf= context.getConfiguration();


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        if(!processed){
            Path filePath =filesplit.getPath();
          //  filePath.getFileSystem(conf).getUri();
            Path [] dir =getInputPath(conf);
            //System.out.println("length:"+dir.length);
           // System.out.println("path:"+dir[0]);
         // FileSystem fs= filePath.getFileSystem(conf);
          //fileStream =fs.open(filePath);
           // this.fileStream= fs.open(filePath);
            this.key=new Text(filePath.getName());
          //  System.out.println(filePath.getName());
            this.value = Highgui.imread(dir[0].toString().substring(5)+"/"+key);
//            byte[] bytes =new byte[(int) filesplit.getLength()];
//            IOUtils.readFully(this.fileStream, bytes, 0, bytes.length);
//
//            this.value= new BytesWritable(bytes);
//            IOUtils.closeStream(fileStream);
            processed =true;
            return true;
        }
        return false;
    }

    private Path[] getInputPath(Configuration conf) {
        // TODO Auto-generated method stub
        String dirs = conf.get("mapred.input.dir","");
        //System.out.println("dirs:"+dirs);
        String[] list = StringUtils.split(dirs);
        Path[] result = new Path[list.length];
        for(int i = 0;i<list.length;i++){
            result[i]=new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return key;
    }

    @Override
    public Mat getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub

        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

}