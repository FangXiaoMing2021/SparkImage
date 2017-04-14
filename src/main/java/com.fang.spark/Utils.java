package com.fang.spark;

import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.highgui.Highgui;

import java.io.*;

/**
 * Created by fang on 16-12-14.
 */
public class Utils {
    public static byte[] serializeObject(Object o) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(o);
            out.close();
            // Get the bytes of the serialized object
            byte[] buf = bos.toByteArray();
            return buf;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }
    }

    public static int[] ByteToObject(byte[] bytes) {
        int[] obj = null;
        try {
            // bytearray to object
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);

            obj = (int[]) oi.readObject();
            bi.close();
            oi.close();
        } catch (Exception e) {
            System.out.println("translation" + e.getMessage());
            e.printStackTrace();
        }
        return obj;
    }

    public static double[][] ByteToTwoArrayHarris(byte[] bytes) {
        double[][] harris = null;
        try {
            // bytearray to object
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);
            harris = (double[][]) oi.readObject();
            bi.close();
            oi.close();
        } catch (Exception e) {
            System.out.println("translation" + e.getMessage());
            e.printStackTrace();
        }
        return harris;
    }

    //使用int，double都出错，改为float
    public static float[] deserializeMat(byte[] b) {
        try {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(b));
            float data[] = (float[]) in.readObject();
            in.close();
            return data;
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
            return null;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }
    }

    public static Mat loadMat(String path) {
        try {
            int cols;
            float[] data;
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path));
            cols = (Integer) ois.readObject();
            data = (float[]) ois.readObject();
            Mat mat = new Mat(data.length / cols, cols, CvType.CV_32F);
            mat.put(0, 0, data);
            return mat;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void saveMat(String path, Mat mat) {
        File file = new File(path).getAbsoluteFile();
        file.getParentFile().mkdirs();
        try {
            int cols = mat.cols();
            float[] data = new float[(int) mat.total() * mat.channels()];
            mat.get(0, 0, data);
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path));
            oos.writeObject(cols);
            oos.writeObject(data);
            oos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static Mat deserializeMat(String path) {
        try {
            int cols;
            float[] data;
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path));
            cols = (Integer) ois.readObject();
            data = (float[]) ois.readObject();
            Mat mat = new Mat(data.length / cols, cols, CvType.CV_32F);
            mat.put(0, 0, data);
            return mat;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //java.lang.UnsupportedOperationException: Mat data type is not compatible: 0
    //没有进行异常处理，出现上面错误，原因是没有提取到特征值，mat为空
    public static byte[] serializeMat(Mat mat) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            float[] data = new float[(int) mat.total() * mat.channels()];
            mat.get(0, 0, data);
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(data);
            out.close();
            // Get the bytes of the serialized object
            byte[] buf = bos.toByteArray();
            return buf;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }
    }

    /**
     * 合并两个Mat对象
     *
     * @param A
     * @param B
     * @return
     */
    Mat mergeRows(Mat A, Mat B) {
        int totalRows = A.rows() + B.rows();

        Mat mergedDescriptors = new Mat();
        Mat submat = mergedDescriptors.rowRange(0, A.rows());
        A.copyTo(submat);
        submat = mergedDescriptors.rowRange(A.rows(), totalRows);
        B.copyTo(submat);
        return mergedDescriptors;
    }

    public static float[] getImageHARRISTwoArray(byte[] image) throws IOException {
        Long startTime = System.currentTimeMillis();
        Mat imageMat = Highgui.imdecode(new MatOfByte(image), Highgui.CV_LOAD_IMAGE_COLOR);
        Mat desc = new Mat();
        FeatureDetector fd = FeatureDetector.create(FeatureDetector.HARRIS);
        MatOfKeyPoint mkp = new MatOfKeyPoint();
        fd.detect(imageMat, mkp);
        DescriptorExtractor de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
        de.compute(imageMat, mkp, desc);
        imageMat.release();
        mkp.release();
        float[] harris;
        if (desc.rows() != 0) {
            harris = new float[(int) desc.total() * desc.channels()];
            desc.get(0, 0, harris);
        } else {
            harris = new float[1];
        }
        System.out.println("获取Harris特征值的时间为:" + (System.currentTimeMillis() - startTime));
        return harris;
    }

    /**
     * 对输入的图像字节数据提取harris特征
     * 该算法经过改进,性能有较大提升
     * 1. 改进了由字节图像数据转换为Mat数据
     * 2. 改进了由Mat类型的特征描述到Double二维数组
     *
     * @param image
     * @return 返回Harris特征描述的二维数组
     * @throws IOException
     */
    public static double[][] getImageHARRISTwoDim(byte[] image) throws IOException {
        Long startTime = System.currentTimeMillis();
        Mat imageMat = Highgui.imdecode(new MatOfByte(image), Highgui.CV_LOAD_IMAGE_COLOR);
        Mat desc = new Mat();
        FeatureDetector fd = FeatureDetector.create(FeatureDetector.HARRIS);
        MatOfKeyPoint mkp = new MatOfKeyPoint();
        fd.detect(imageMat, mkp);
        DescriptorExtractor de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
        de.compute(imageMat, mkp, desc);

        Mat doubleMat = new Mat();
        //desc.copyTo(doubleMat);
        double[][] twoDimDoubleArray = new double[desc.rows()][desc.cols()];
        if (desc.rows() != 0) {
            //将CvType.CV_32F转换为CvType.CV_64F
            //解决错误java.lang.UnsupportedOperationException: Mat data type is not compatible: 5
            desc.convertTo(doubleMat, CvType.CV_64F);
            for (int i = 0; i < desc.rows(); i++) {
                double[] tmp = new double[desc.cols()];
                doubleMat.get(i, 0, tmp);
                twoDimDoubleArray[i] = tmp;
            }
        }
//        }else{
//            System.out.println("没有提取到Harris特征值");
//        }

        imageMat.release();
        mkp.release();
        doubleMat.release();
        //使用本地方法,提高速度 System.arraycopy(tmp,i*128,xs,0,128)
        System.out.println("getImageHARRISTwoDim,获取Harris特征值的时间为:" + (System.currentTimeMillis() - startTime));
        return twoDimDoubleArray;
    }
}
