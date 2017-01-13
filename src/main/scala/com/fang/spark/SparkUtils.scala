package com.fang.spark

import java.io._
import javax.imageio.ImageIO

import org.opencv.core.Mat
import sun.misc.{BASE64Decoder, BASE64Encoder}

/**
  * Created by fang on 16-12-16.
  */
object SparkUtils {
  private[spark] val encoder = new BASE64Encoder
  private[spark] val decoder = new BASE64Decoder
  //使用int，double都出错，改为float
  //将byte[]数组反序列化为float[]
  def deserializeMatArray(b: Array[Byte]): Array[Float] =
  {
    try {
      val in = new ObjectInputStream(new ByteArrayInputStream(b))
      val data = in.readObject.asInstanceOf[Array[Float]]
      in.close()
      data
    }
    catch {
      case cnfe: ClassNotFoundException => {
        cnfe.printStackTrace()
        null
      }
      case ioe: IOException => {
        ioe.printStackTrace()
        null
      }
    }
  }
  //java.lang.UnsupportedOperationException: Mat data type is not compatible: 0
  //没有进行异常处理，出现上面错误，原因是没有提取到特征值，mat为空
  //将Mat序列化为byte[]
  def serializeMat(mat: Mat): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    try {
      val data = new Array[Float](mat.total.toInt * mat.channels)
      mat.get(0, 0, data)
      val out = new ObjectOutputStream(bos)
      out.writeObject(data)
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

  def printComputeTime(beginTime:Long,message:String) = {
    println("***************************************************************")
    println(message+" 耗时: " + (System.currentTimeMillis() - beginTime)+"ms")
    println("***************************************************************")
  }

   def getImageToString(file: File) :String = {
     var imageString :String = ""
    try {
      val bi = ImageIO.read(file)
      val baos = new ByteArrayOutputStream
      ImageIO.write(bi, "jpg", baos)
      val bytes = baos.toByteArray
      imageString =encoder.encodeBuffer(bytes).trim
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
    imageString
  }

  def base64StringToImage(base64String: String, fileName: String) {
    try {
      val bytes1 = decoder.decodeBuffer(base64String)
      val bais = new ByteArrayInputStream(bytes1)
      val bi1 = ImageIO.read(bais)
      //            bi1.flush();
      //            bais.close();
      val w2 = new File("/home/fang/images/" + fileName) //可以是jpg,png,gif格式
      ImageIO.write(bi1, "jpg", w2) //不管输出什么格式图片，此处不需改动
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }
}
