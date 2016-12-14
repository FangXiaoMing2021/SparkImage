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

}
