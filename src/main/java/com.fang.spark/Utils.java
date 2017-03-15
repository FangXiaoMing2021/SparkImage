package com.fang.spark;

import org.opencv.core.CvType;
import org.opencv.core.Mat;

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

            obj = (int[])oi.readObject();
            bi.close();
            oi.close();
        } catch (Exception e) {
            System.out.println("translation" + e.getMessage());
            e.printStackTrace();
        }
        return obj;
    }

    //使用int，double都出错，改为float
    public static float[] deserializeMat(byte[] b) {
        try {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(b));
            float data[] = (float[])in.readObject();
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
     * @param A
     * @param B
     * @return
     */
    Mat mergeRows(Mat A, Mat B)
    {
        int totalRows = A.rows() + B.rows();

        Mat mergedDescriptors=new Mat();
        Mat submat = mergedDescriptors.rowRange(0, A.rows());
        A.copyTo(submat);
        submat = mergedDescriptors.rowRange(A.rows(), totalRows);
        B.copyTo(submat);
        return mergedDescriptors;
    }

}
