package com.fang.spark.demo;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.core.Scalar;
import org.opencv.features2d.FeatureDetector;
import org.opencv.features2d.Features2d;
import org.opencv.highgui.Highgui;

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;

/**
 * Created by fang on 17-1-12.
 */
public class DrowKeyPoint {
    public static void  main(String args[]){
        // Features SEARCH
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        Mat image = Highgui.imread("/home/fang/images/me.jpg");
        int detectorType = FeatureDetector.SIFT;
        FeatureDetector detector = FeatureDetector.create(detectorType);
        Mat mask = new Mat();
        MatOfKeyPoint keypoints = new MatOfKeyPoint();
        detector.detect(image, keypoints , mask);
        if (!detector.empty()){
            // Draw kewpoints
            Mat outputImage = new Mat();
            Scalar color = new Scalar(0, 0, 255); // BGR
            // For each keypoint, the circle around keypoint with keypoint size and orientation will be drawn.
            int flags = Features2d.DRAW_RICH_KEYPOINTS;
            //Features2d.drawKeypoints(image, keypoints, outputImage);
            Features2d.drawKeypoints(image, keypoints, outputImage, color , flags);
            //BufferedImage bi = ImageIO.read(new ByteArrayInputStream(outputImage))
            displayImage(Mat2BufferedImage(outputImage));
        }
    }
    public static BufferedImage Mat2BufferedImage(Mat m) {
        // Fastest code
        // output can be assigned either to a BufferedImage or to an Image

        int type = BufferedImage.TYPE_BYTE_GRAY;
        if ( m.channels() > 1 ) {
            type = BufferedImage.TYPE_3BYTE_BGR;
        }
        int bufferSize = m.channels()*m.cols()*m.rows();
        byte [] b = new byte[bufferSize];
        m.get(0,0,b); // get all the pixels
        BufferedImage image = new BufferedImage(m.cols(),m.rows(), type);
        final byte[] targetPixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
        System.arraycopy(b, 0, targetPixels, 0, b.length);
        return image;
    }
    public static void displayImage(Image img2) {

        //BufferedImage img=ImageIO.read(new File("/HelloOpenCV/lena.png"));
        ImageIcon icon=new ImageIcon(img2);
        JFrame frame=new JFrame();
        frame.setLayout(new FlowLayout());
        frame.setSize(img2.getWidth(null)+50, img2.getHeight(null)+50);
        JLabel lbl=new JLabel();
        lbl.setIcon(icon);
        frame.add(lbl);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }
}
