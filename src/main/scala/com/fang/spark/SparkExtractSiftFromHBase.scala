package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}

/**
  * Created by fang on 16-12-13.
  */
object SparkExtractSiftFromHBase {
  def main(args:Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkExtractSiftFromHBase").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val tableName = TableName.valueOf("imagesTable")
    val hbaseConfig = HBaseConfiguration.create
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableName.toString)
    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig)
    val admin = connection.getAdmin()
    val hBaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.foreach {
      tuple => {
        //val key = tuple._1.get()
        val value = tuple._2
        val image = value.getColumnCells(Bytes.toBytes("image"), Bytes.toBytes("img")).get(0).getValueArray
        val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
        val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
        val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
        test_mat.put(0, 0, data)
        val desc = new Mat
        val fd = FeatureDetector.create(FeatureDetector.SIFT)
        val mkp = new MatOfKeyPoint
        fd.detect(test_mat, mkp)
        val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
        de.compute(test_mat, mkp, desc) //提取sift特征
        desc.toString
        val imagesTable: Table = connection.getTable(tableName)
        val put: Put = new Put(value.getRow)
        put.addImmutable(Bytes.toBytes("imagesTable"), Bytes.toBytes("image"), Bytes.toBytes(desc.toString))
        imagesTable.put(put)
      }
    }
    sparkContext.stop()
    admin.close()
  }
}


