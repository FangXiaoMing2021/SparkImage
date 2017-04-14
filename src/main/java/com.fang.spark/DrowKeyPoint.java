package com.fang.spark;

import org.apache.commons.io.FileUtils;
import org.opencv.core.*;
import org.opencv.features2d.FeatureDetector;
import org.opencv.features2d.Features2d;
import org.opencv.highgui.Highgui;

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;

/**
 * Created by fang on 17-1-12.
 */
public class DrowKeyPoint {
    private static final int IMAGE_WIDTH = 800;
    public static void  main(String args[]) throws IOException {
        // Features SEARCH
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        //n01751748_5705没有sift特征点
        //Mat image = Highgui.imread("/home/fang/images/train/3/n01751748_7878.JPEG");
        File file = new File("/home/fang/images/train/3/n01751748_7878.JPEG");
        byte[] imageData = FileUtils.readFileToByteArray(file);
        Mat image = Highgui.imdecode(new MatOfByte(imageData), Highgui.CV_LOAD_IMAGE_UNCHANGED);

//        File imageFile =new File("/home/fang/images/n01984695_16712.JPEG");
//        BufferedImage bi = ImageIO.read(imageFile);
//        Mat image = new Mat(bi.getHeight(), bi.getWidth(), CvType.CV_8UC1);
//        byte[] data = ((DataBufferByte)bi.getRaster().getDataBuffer()).getData();
//        image.put(0, 0, data);

//        MatOfByte matOfByte = new MatOfByte();
//
//        // encoding to png, so that your image does not lose information like with jpeg.
//        Highgui.imencode(".png", mGray, matOfByte);
//
//        byte[] byteArray = matOfByte.toArray();

        //int detectorType = FeatureDetector.GFTT;
        //int detectorType = FeatureDetector.HARRIS;
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
        int height = (IMAGE_WIDTH*img2.getHeight(null))/img2.getWidth(null);
        ImageIcon icon=new ImageIcon(img2.getScaledInstance(IMAGE_WIDTH,height,Image.SCALE_SMOOTH));
        //BufferedImage img=ImageIO.read(new File("/HelloOpenCV/lena.png"));
        //ImageIcon icon=new ImageIcon(img2);
        JFrame frame=new JFrame();
        frame.setLayout(new FlowLayout());
        frame.setSize(img2.getWidth(null)/2, img2.getHeight(null)/2);
        JLabel lbl=new JLabel();
        lbl.setIcon(icon);
        frame.add(lbl);
//        JLabel lbl1=new JLabel();
//        lbl1.setIcon(icon);
//        JLabel lbl2=new JLabel();
//        lbl2.setIcon(icon);
//        JLabel lbl3=new JLabel();
//        lbl3.setIcon(icon);
//        JLabel lbl4=new JLabel();
//        lbl4.setIcon(icon);
//        JLabel lbl5=new JLabel();
//        lbl5.setIcon(icon);
//        frame.add(lbl);
//        frame.add(lbl1);
//        frame.add(lbl2);
//        frame.add(lbl3);
//        frame.add(lbl4);
//        frame.add(lbl5);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }
}
