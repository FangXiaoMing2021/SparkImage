package com.fang.spark;

import org.apache.commons.io.FileUtils;
import org.opencv.core.*;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.highgui.Highgui;

import java.io.File;
import java.io.IOException;

/**
 * Created by fang on 17-4-12.
 */
public class ImdecodeImageTest {
    public static void main(String[] args) throws IOException {

        //Mat image = Highgui.imread("/home/fang/images/2.JPEG");
        File file = new File("/home/fang/images/2.JPEG");

        byte[] imageData = FileUtils.readFileToByteArray(file);
        //耗时10ms
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        Long startTime = System.currentTimeMillis();

        Mat image = Highgui.imdecode(new MatOfByte(imageData), Highgui.CV_LOAD_IMAGE_UNCHANGED);
        Mat  desc = new Mat();
        FeatureDetector fd = FeatureDetector.create(FeatureDetector.HARRIS);
        MatOfKeyPoint mkp = new MatOfKeyPoint();
        fd.detect(image, mkp);
        DescriptorExtractor de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
        de.compute(image, mkp, desc);
        image.release();
        mkp.release();
        System.out.println("特征个数:"+desc.rows());

        Long startFirst = System.currentTimeMillis();
        Mat doubleMat = new Mat();
        desc.convertTo(doubleMat,CvType.CV_64F);

        //desc.copyTo(doubleMat);
        double[][] twoDimDoubleArray = new double[desc.rows()][desc.cols()];
        for(int i=0;i<desc.rows();i++){
            double[] tmp = new double[128];
            doubleMat.get(i,0,tmp);
            twoDimDoubleArray[i] = tmp;
        }
        double[][] test = new double[0][0];
        System.out.println("third:"+(System.currentTimeMillis()-startFirst));

        System.out.println(doubleMat.get(1,0)[0]);

        System.out.println("运行总时间:"+(System.currentTimeMillis()-startTime));
    }
}
