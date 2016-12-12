package com.fang.spark;

/**
 * Created by fang on 16-12-12.
 */

import org.apache.spark.input.PortableDataStream;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.IOException;

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

    public static Mat bufferedImageToMat(BufferedImage bi) {
        Mat mat = new Mat(bi.getHeight(), bi.getWidth(), CvType.CV_8UC3);
        byte[] data = ((DataBufferByte) bi.getRaster().getDataBuffer()).getData();
        mat.put(0, 0, data);
        return mat;
    }
}