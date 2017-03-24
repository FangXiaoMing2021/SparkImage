package com.fang.spark

import java.awt.image.BufferedImage
import java.io._
import java.util.HashMap
import javax.imageio.ImageIO
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
/**
  * Created by fang on 16-12-21.
  * 启动kafka: bin/kafka-server-start.sh config/server.properties &
  * 该代码实现读取本地图像文件夹,然后使用kafka发送消息
  */
object KafkaImageProducer {
  def main(args: Array[String]) {
    val topic = "image_topic"
    val brokers = "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
    // Zookeeper connection properties
    // 生产端配置的是kafka的ip和端口
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,Array[Byte]](props)
    val fileList = new File("/home/fang/imageTest").listFiles()
    println("图片总数为:"+fileList.length)
    for(i<- 0 to fileList.length-1 ){
      val file = fileList(i)
      val bi:BufferedImage= ImageIO.read(file)
      val out = new ByteArrayOutputStream
      val flag = ImageIO.write(bi, "jpg", out)
      val image = out.toByteArray
      val message = new ProducerRecord[String, Array[Byte]](topic, file.getName, image)
      producer.send(message)
      println(file.getName+" 时间 "+System.currentTimeMillis())
      //Thread.sleep(100)
    }

    producer.close()
  }
}
