package com.fang.spark.demo;

/**
 * Created by fang on 16-12-12.
 */

import org.apache.spark.input.PortableDataStream;
import org.opencv.core.*;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class ExtractSift {
    public static void sift(PortableDataStream portableDataStream)throws IOException {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
//        Mat test_mat = Highgui.imread("/home/fang/Pictures/1.jpg");
        BufferedImage bi = ImageIO.read(new ByteArrayInputStream(portableDataStream.toArray()));
        Mat test_mat = bufferedImageToMat(bi);
        Mat desc = new Mat();
        FeatureDetector fd = FeatureDetector.create(FeatureDetector.SIFT);
        MatOfKeyPoint mkp = new MatOfKeyPoint();
        fd.detect(test_mat, mkp);
        DescriptorExtractor de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
        de.compute(test_mat, mkp, desc);//提取sift特征
        System.out.println(desc.cols());
        System.out.println(desc.rows());
    }
    //BufferedImage transform to Mat
    public static Mat bufferedImageToMat(BufferedImage bi) {
        Mat mat = new Mat(bi.getHeight(), bi.getWidth(), CvType.CV_8UC3);
        byte[] data = ((DataBufferByte) bi.getRaster().getDataBuffer()).getData();
        mat.put(0, 0, data);
        return mat;
    }
    public static void siftFromBinary(byte[] image)throws IOException {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        BufferedImage bi = ImageIO.read(new ByteArrayInputStream(image));
        Mat test_mat = bufferedImageToMat(bi);
        Mat desc = new Mat();
        FeatureDetector fd = FeatureDetector.create(FeatureDetector.SIFT);
        MatOfKeyPoint mkp = new MatOfKeyPoint();
        fd.detect(test_mat, mkp);
        DescriptorExtractor de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
        de.compute(test_mat, mkp, desc);//提取sift特征
        System.out.println(desc.cols());
        System.out.println(desc.rows());
    }

    public static void stackoverflow(int h,int w,byte[]array) throws IOException{
        Mat mat = new Mat(h,w, CvType.CV_8U);

        mat.put(0, 0, array);

        //Imshow is = new Imshow("try");for verification

        MatOfPoint2f quad = new MatOfPoint2f(mat);//array of corner points

        MatOfPoint2f rect = new MatOfPoint2f(mat);//final array of corner points into which mat should be warped into

        Mat transmtx = Imgproc.getPerspectiveTransform(quad,rect);

        Mat output = new Mat(w,h,CvType.CV_8U);

        Imgproc.warpPerspective(mat, output, transmtx, new Size(w,h),Imgproc.INTER_CUBIC);

        //is.showImage(output);

        MatOfByte matOfByte = new MatOfByte();

        Highgui.imencode(".jpg", output, matOfByte);

        byte[] byteArray = matOfByte.toArray();

        File f = new File("retrieve1.jpg");

        BufferedImage img1 =null;

        InputStream in = new ByteArrayInputStream(byteArray);

        img1  = ImageIO.read(in);

        WritableRaster raster = (WritableRaster)img1.getData();

        raster.setDataElements(0,0,byteArray);

        img1.setData(raster);
    }
}