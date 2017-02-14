package com.fang.spark.bakeup

import java.io._
import java.util.{HashMap, Properties}
import javax.imageio.ImageIO

import com.fang.spark.{ImageMember, ObjectEncoder, SparkUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by fang on 16-12-21.
  */
object KafkaImageProducer {
  def main(args: Array[String]) {
    val topic = "image_topic"
    val brokers = "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
    // Zookeeper connection properties
    // 生产端配置的是kafka的ip和端口
    val props = new Properties()
    props.setProperty("zookeeper.connect", "218.199.92.225:2181") //这里根据实际情况填写你的zk连接地址
    props.setProperty("bootstrap.servers", "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092") //根据自己的配置填写连接地址
    props.setProperty("serializer.class", classOf[ObjectEncoder].getName) //填写刚刚自定义的Encoder类
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    val producer = new KafkaProducer[String,Object](props)
    val fileList = new File("/home/fang/imageTest").listFiles()
    for(file<-fileList){
      val bi = ImageIO.read(file)
      val out = new ByteArrayOutputStream
      val flag = ImageIO.write(bi, "jpg", out)
      val image = out.toByteArray
      val imageMember = new ImageMember(file.getName, image)
      val message = new ProducerRecord[String, Object](topic, file.getName, imageMember)
      println(file.getName)
      println(image.length)
      producer.send(message)
      Thread.sleep(100)
    }
    producer.close()
  }
}
