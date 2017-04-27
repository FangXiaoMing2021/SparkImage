package com.fang.spark

import java.awt.image.BufferedImage
import java.io._
import java.util.HashMap
import javax.imageio.ImageIO

import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by fang on 16-12-21.
  * 启动kafka: bin/kafka-server-start.sh config/server.properties &
  * ./bin/kafka-server-start.sh config/server.properties >> log.txt &
  * 该代码实现读取本地图像文件夹,然后使用kafka发送消息
  */
object KafkaImageProducer {
  def main(args: Array[String]) {
    val topic = "image_topic"
    val brokers = "fang-ubuntu:9092,fei-ubuntu:9092,kun-ubuntu:9092"
    // Zookeeper connection properties
    // 生产端配置的是kafka的ip和端口
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    //val fileList = new File("/home/fang/images/test").listFiles()
    val fileList = new File("/home/user/n02009229").listFiles()
    println("图片总数为:" + fileList.length)
    for(i<- 100 to 200){
    //for (i <- 0 to fileList.length - 1) {
      val file = fileList(i)
      val image: Array[Byte] = FileUtils.readFileToByteArray(file)
      val message = new ProducerRecord[String, Array[Byte]](topic, file.getName, image)
      producer.send(message)
      println(file.getName + " 时间 " + System.currentTimeMillis())
      //i7 8G两台Worker节点可以接受10/s，批次间隔1s
      //i7 8G两台Worker节点,开启10线程，可以接受35/s，批次间隔1s
      //开启多线程，四台Worker机电可以接受33/s，批次间隔1s
      Thread.sleep(20)
    }

    producer.close()
  }

  def bufferedImageRead(file: File): Array[Byte] = {
    val bi: BufferedImage = ImageIO.read(file)
    val out = new ByteArrayOutputStream
    //val flag = ImageIO.write(bi, "jpg", out)
    val image = out.toByteArray
    image
  }
}
