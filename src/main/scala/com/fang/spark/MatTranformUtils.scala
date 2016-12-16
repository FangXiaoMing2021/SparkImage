package com.fang.spark

import java.io._

import org.opencv.core.Mat

/**
  * Created by fang on 16-12-16.
  */
object MatTranformUtils {
  //使用int，double都出错，改为float
  //将byte[]数组反序列化为float[]
  def deserializeMatArray(b: Array[Byte]): Array[Float] = {
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
}
