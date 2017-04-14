package com.fang.spark;

import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;

/**
 * Created by fang on 17-4-12.
 */
public class BufferedImageTest {
    public static void main(String[] args) throws IOException {
       // Long startTime = System.currentTimeMillis();
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
       // Mat image = Highgui.imread("/home/fang/images/train/3/n01751748_7878.JPEG");
        File imageFile =new File("/home/fang/images/2.JPEG");
        BufferedImage bi = ImageIO.read(imageFile);

        Mat imageMat = new Mat(bi.getHeight(), bi.getWidth(), CvType.CV_8UC3);
        byte[] data = ((DataBufferByte)bi.getRaster().getDataBuffer()).getData();
        Long startTime = System.currentTimeMillis();
        ImagesUtil.getImageHARRIS(data);
//        imageMat.put(0, 0, data);
//        int detectorType = FeatureDetector.HARRIS;
//
//        FeatureDetector detector = FeatureDetector.create(detectorType);
//        Mat desc = new Mat();
//        MatOfKeyPoint mkp = new MatOfKeyPoint();
//        detector.detect(imageMat, mkp);
//        DescriptorExtractor de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
//        de.compute(imageMat, mkp, desc);
//        imageMat.release();
//        mkp.release();
//
//        System.out.println(desc.rows());
//        System.out.println(desc.cols());
//        Utils.serializeMat(desc);
//        //使用double 出现Exception in thread "main" java.lang.UnsupportedOperationException: Mat data type is not compatible: 5
//        float[] harris;
//        //double[][] harrasDoubleArray;
//        if (desc.rows() != 0) {
//            harris = new float[(int) desc.total() * desc.channels()];
//            desc.get(0, 0, harris);
//        }else{
//            harris = new float[1];
//        }
        System.out.println("运行时间"+(System.currentTimeMillis()-startTime));
    }
}
