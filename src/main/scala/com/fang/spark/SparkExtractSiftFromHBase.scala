/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.fang.spark
import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectOutputStream}
import javax.imageio.ImageIO

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.spark._
import org.opencv.core.{Core, CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}
object SparkExtractSiftFromHBase {
  //不使用成员变量的方式会出现 conf not serialization
  private[spark] val conf = HBaseConfiguration.create
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val tableName = "imagesTest"
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    /*
      *  没加载 System.loadLibrary(Core.NATIVE_LIBRARY_NAME)导致下面的错误，MD
      *  UnsatisfiedLinkError: Native method not found: org.opencv.core.Mat.n_Mat:()J
      */
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    hBaseRDD.foreachPartition {
      iter => {
        /*
        *没有使用foreachPartition出现下面的错误
        * org.apache.spark.SparkException: Task not serializable
         */
        val connection:Connection= ConnectionFactory.createConnection(conf)
        iter.foreach {
          tuple => {
            val result = tuple._2
            //putSiftIntoHBase(tableName,result,connection)
            getSiftFromHBase(tableName,result,connection)
//            val image = result.getValue(Bytes.toBytes("image"), Bytes.toBytes("binary"))
//            val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
//            val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
//            val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
//            test_mat.put(0, 0, data)
//            val desc = new Mat
//            val fd = FeatureDetector.create(FeatureDetector.SIFT)
//            val mkp = new MatOfKeyPoint
//            fd.detect(test_mat, mkp)
//            val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
//            de.compute(test_mat, mkp, desc) //提取sift特征
//            val imagesTable: Table = connection.getTable(TableName.valueOf(tableName))
//            val put: Put = new Put(result.getRow)
//            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"),Utils.serializeMat(desc))
//            imagesTable.put(put)
          }
        }
        connection.close()
      }
    }
    sc.stop()
  }

  def putSiftIntoHBase(tableName:String,result:Result, connection:Connection): Unit ={
    val image = result.getValue(Bytes.toBytes("image"), Bytes.toBytes("binary"))
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
    val imagesTable: Table = connection.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(result.getRow)
    put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"),Utils.serializeMat(desc))
    imagesTable.put(put)
  }

  def getSiftFromHBase(tableName:String,result:Result,connection:Connection): Unit ={
    val siftByte= result.getValue(Bytes.toBytes("image"),Bytes.toBytes("sift"))
    val siftArray:Array[Float]=Utils.deserializeMat(siftByte)
    siftArray.foreach{
      i=>
        print(i+" ")
        if(i % 128==0){
          println()
        }
    }
  }

  def serializeObject(o: Any): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    try {
      val out = new ObjectOutputStream(bos)
      out.writeObject(o)
      out.close()
      // Get the bytes of the serialized object
      val buf = bos.toByteArray
      buf
    }
    catch {
      case ioe: IOException => {
        ioe.printStackTrace()
        null
      }
    }
  }
}