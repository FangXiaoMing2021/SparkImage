package com.fang.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 16-11-24.
 * 从HBase中获取相似图像,并在界面中显示
 * 涉及到两张表,imagesTest和similarImageTable
 * 最多显示10张相似图像
 */
public class SimilarImageView extends JFrame implements ActionListener{
    private  Configuration cfg = HBaseConfiguration.create();
    private  int index = 0;
    private  Button nextButton = new Button("下一张");
    private  List<byte[]> similarImageByteList = new ArrayList<byte[]>();
    private  List<Get> similarImageNameList;
    private  List<JLabel> labelList = new ArrayList<JLabel>();
    private Table imageTable;
    private Table similarImageTable;
    private static final int NUMBER_OF_SIMILAR_IMAGE = 10;
    private static final String IMAGE_TABLE = "imagesTest";
    private static final String SIMILAR_IMAGE_TABLE = "similarImageTable";
    private static final int FIRST_INDEX = 0;
    private static final int IMAGE_WIDTH = 120;
    private static final int IMAGE_HEIGHT = 160;
    private static final String SIMILAR_COLUMN_FAMILY = "similarImage";

    public static void main(String args[]) throws IOException {
        TableName imageTableName = TableName.valueOf(IMAGE_TABLE);
        TableName similarImageTableName = TableName.valueOf(SIMILAR_IMAGE_TABLE);
        SimilarImageView view = new SimilarImageView();
        //连接HBase配置
        view.cfg.set("hbase.zookeeper.property.clientPort", "2181");
        view.cfg.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu");
        Connection connection = ConnectionFactory.createConnection(view.cfg);
        view.imageTable = connection.getTable(imageTableName);
        //从相似图像表中获取相似图像名称
        view.similarImageTable = connection.getTable(similarImageTableName);
        //待查询的图片名称
        //String key = "n02892767_5106.JPEG";
        //从相似表中获取所有已经匹配的图像名称
        view.similarImageNameList = view.getSimilarTableRowKey(view.similarImageTable);
        //final Get g = new Get(Bytes.toBytes(key));
        view.setTitle("相似图像"+"("+view.similarImageNameList.size()+")");
        view.setLayout(new FlowLayout());
        view.setSize(IMAGE_WIDTH*3+50, IMAGE_HEIGHT *4+50);
        view.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        view.nextButton.setSize(IMAGE_WIDTH*3+50,40);
        view.add(view.nextButton);
        for(int i=FIRST_INDEX;i<NUMBER_OF_SIMILAR_IMAGE;i++){
            JLabel lbl=new JLabel();
            view.labelList.add(lbl);
            view.add(lbl);
        }
        view.setVisible(true);
        //view.setLayout(new GridLayout(3,4));
        //view.showNextImage(view.similarImageTable,view.imageTable,view.similarImageNameList.get(view.index));
        view.nextButton.addActionListener(view);
    }

    /**
     * 响应Button点击事件
     * @param e
     */

    public void actionPerformed(ActionEvent e) {
        try {
            this.similarImageByteList.clear();
            this.showNextImage(similarImageTable,imageTable,similarImageNameList.get(this.index));
            this.setTitle("相似图像"+"("+this.index+"/"+this.similarImageNameList.size()+")");
            this.index=(this.index+1)%similarImageNameList.size();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    /**
     * 获取ImageTable中的图像数据,将获取的二进制数据放入List中
     * @param similarImageTable
     * @param imageTable
     * @param g
     * @throws IOException
     */

    public void showNextImage(Table similarImageTable,Table imageTable,Get g) throws IOException {
        Result result = similarImageTable.get(g);
        List<byte[]> similarImageNameByteList = new ArrayList<byte[]>();
        for(int i=FIRST_INDEX;i<NUMBER_OF_SIMILAR_IMAGE;i++){
            String similarColumnFamily = "image_"+String.valueOf(i+1);
            byte[] similarImageByte = result.getValue(Bytes.toBytes(SIMILAR_COLUMN_FAMILY), Bytes.toBytes(similarColumnFamily));
            similarImageNameByteList.add(similarImageByte);
        }


        List<Get> similarImageGetList = new ArrayList<Get>();
        List<String> similarImageDistance = new ArrayList<String>();
        //将原图放入
        similarImageGetList.add(g);
        similarImageDistance.add("原图:"+this.index);
        for(int i=FIRST_INDEX;i<NUMBER_OF_SIMILAR_IMAGE;i++){
            byte[] similarImageByte=similarImageNameByteList.get(i);
            String imageName = Bytes.toString(similarImageByte);
            String[] similarImageInfo = imageName.split("#");
            Get get = new Get(Bytes.toBytes(similarImageInfo[FIRST_INDEX]));
            similarImageGetList.add(get);
            similarImageDistance.add("SIFT距离:"+similarImageInfo[FIRST_INDEX+1]);
        }


        Result[] imageResult = imageTable.get(similarImageGetList);
        for(Result similarImage:imageResult){
            byte[] imageBinary = similarImage.getValue(Bytes.toBytes("image"), Bytes.toBytes("binary"));
            similarImageByteList.add(imageBinary);
        }
        displayImage(this,similarImageByteList,similarImageDistance);
    }

    /**
     * 在界面中显示相似图像,总共10张相似图像,一张原图
     * @param similarImageByteList
     * @throws IOException
     */
    public void displayImage(SimilarImageView frame,List<byte[]> similarImageByteList,List<String> similarImageDistance) throws IOException {
        for(int i=FIRST_INDEX;i<NUMBER_OF_SIMILAR_IMAGE;i++){
            BufferedImage bi = ImageIO.read(new ByteArrayInputStream(similarImageByteList.get(i)));
            int height = (IMAGE_WIDTH*bi.getHeight())/bi.getWidth();
            ImageIcon icon=new ImageIcon(bi.getScaledInstance(IMAGE_WIDTH,height,Image.SCALE_SMOOTH));
            JLabel lbl=frame.labelList.get(i);
            lbl.setHorizontalTextPosition(0);
            lbl.setForeground(Color.white);
            lbl.setText(similarImageDistance.get(i));
            lbl.setSize(IMAGE_WIDTH,IMAGE_HEIGHT);
            lbl.setIcon(icon);
        }
//        for(byte[] similarImageByte:similarImageByteList){
//            BufferedImage bi = ImageIO.read(new ByteArrayInputStream(similarImageByte));
//            int height = (IMAGE_WIDTH*bi.getHeight())/bi.getWidth();
//            ImageIcon icon=new ImageIcon(bi.getScaledInstance(IMAGE_WIDTH,height,Image.SCALE_SMOOTH));
//            JLabel lbl=new JLabel();
//            lbl.setHorizontalTextPosition(0);
//            lbl.setForeground(Color.white);
//            lbl.setText("image");
//            lbl.setSize(120,160);
//            lbl.setIcon(icon);
//            frame.add(lbl);
//        }
//        frame.setVisible(true);
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    /**
     * 获取相似表中所有的rowkey
     * @param similarTable
     * @return rowKeyList
     * @throws IOException
     */

    public List<Get> getSimilarTableRowKey(Table similarTable) throws IOException {
        Scan scan = new Scan();
        ResultScanner rss = similarTable.getScanner(scan);
        List<Get> rowKeyList = new ArrayList<Get>();
        for(Result result:rss){
            Get get = new Get(result.getRow());
            rowKeyList.add(get);
        }
        return rowKeyList;
    }
}
