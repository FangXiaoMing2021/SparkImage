package com.fang.spark.demo;

/**
 * Created by fang on 17-2-13.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
@SuppressWarnings("deprecation")
public class KafkaProducerObject {
    public static void main(String[] args) {
        String topic = "test"; // 定义要操作的主题
        Properties pro = new Properties(); // 定义相应的属性保存
        pro.setProperty("zookeeper.connect", "218.199.92.225:2181"); //这里根据实际情况填写你的zk连接地址
        pro.setProperty("metadata.broker.list", "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"); //根据自己的配置填写连接地址
        pro.setProperty("serializer.class", ObjectEncoder.class.getName()); //填写刚刚自定义的Encoder类
        Producer<Integer, Object> prod = new Producer<Integer, Object>(new ProducerConfig(pro));
        File[] fileList = new File("/home/fang/imageTest").listFiles();
        for(File file:fileList){
            System.out.println(file.getName());
            try {
                BufferedImage bi = ImageIO.read(file);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                boolean flag = ImageIO.write(bi, "jpg", out);
                byte[] image = out.toByteArray();
                ImageMember imageMember = new ImageMember(file.getName(),image);
                prod.send(new KeyedMessage<Integer, Object>(topic, imageMember));  //测试发送对象数据
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        prod.close();
        //prod.send(new KeyedMessage<Integer, Object>(topic, new Member("姓名",12,new Date(),12.1)));  //测试发送对象数据
    }

}