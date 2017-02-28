package com.fang.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 16-11-24.
 */
public class SimilarImageView {
    private static Configuration cfg = HBaseConfiguration.create();
    private static final int NUMBER_OF_SIMILAR_IMAGE = 10;
    private static final String IMAGE_TABLE = "imagesTest";
    private static final String SIMILAR_IMAGE_TABLE = "similarImageTable";
    private static final int FIRST_INDEX = 0;
    public static void main(String args[]) throws IOException {
        TableName imageTableName = TableName.valueOf(IMAGE_TABLE);
        TableName similarImageTableName = TableName.valueOf(SIMILAR_IMAGE_TABLE);
        String columnFamily = "similarImage";

        //连接HBase配置
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu");
        Connection connection = ConnectionFactory.createConnection(cfg);

        //从相似图像表中获取相似图像名称
        Table similarImageTable = connection.getTable(similarImageTableName);
        //待查询的图片名称
        String key = "3.jpg";
        Get g = new Get(Bytes.toBytes(key));
        Result result = similarImageTable.get(g);
        List<byte[]> similarImageNameByteList = new ArrayList<byte[]>();
        for(int i=FIRST_INDEX;i<NUMBER_OF_SIMILAR_IMAGE;i++){
            String similarColumnFamily = "image_"+String.valueOf(i+1);
            byte[] similarImageByte = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(similarColumnFamily));
            similarImageNameByteList.add(similarImageByte);
        }


        List<Get> similarImageGetList = new ArrayList<Get>();
        for(int i=FIRST_INDEX;i<NUMBER_OF_SIMILAR_IMAGE;i++){
            byte[] similarImageByte=similarImageNameByteList.get(i);
            String imageName = Bytes.toString(similarImageByte);
            Get get = new Get(Bytes.toBytes(imageName.split("#")[FIRST_INDEX]));
            similarImageGetList.add(get);
        }

        Table imageTable = connection.getTable(imageTableName);
        Result[] imageResult = imageTable.get(similarImageGetList);
        List<byte[]> similarImageByteList = new ArrayList<byte[]>();
        for(Result similarImage:imageResult){
            byte[] imageBinary = similarImage.getValue(Bytes.toBytes("image"), Bytes.toBytes("binary"));
            System.out.println(imageBinary.length);
            similarImageByteList.add(imageBinary);
        }
        displayImage(similarImageByteList);
    }

    /**
     * 在界面中显示相似图像,总共10张相似图像,一张原图
     * @param similarImageByteList
     * @throws IOException
     */
    public static void displayImage(List<byte[]> similarImageByteList) throws IOException {

        BufferedImage img= ImageIO.read(new ByteArrayInputStream(similarImageByteList.get(0)));
        JFrame frame=new JFrame();
        frame.setLayout(new FlowLayout());
        frame.setSize(img.getWidth(null)*4+50, img.getHeight(null)*4+50);
        for(byte[] similarImageByte:similarImageByteList){
            BufferedImage bi = ImageIO.read(new ByteArrayInputStream(similarImageByte));
            ImageIcon icon=new ImageIcon(bi);
            JLabel lbl=new JLabel();
            lbl.setIcon(icon);
            frame.add(lbl);
        }
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }
}
