package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io._
import java.util.HashMap
import javax.imageio.ImageIO

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
/**
  * Created by fang on 16-12-21.
  */
object KafkaImageProducer {
  def main(args: Array[String]) {
    val topic = "image_topic"
    val brokers = "218.199.92.244:9092"
    // Zookeeper connection properties
    // 生产端配置的是kafka的ip和端口
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val fileList = new File("/home/fang/images").listFiles()
    for(file<-fileList){
      val bi:BufferedImage= ImageIO.read(file)
      val image = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
      val message = new ProducerRecord[String, Array[Byte]](topic, file.getName, image)
      producer.send(message)
      Thread.sleep(100)
    }
  }
}
