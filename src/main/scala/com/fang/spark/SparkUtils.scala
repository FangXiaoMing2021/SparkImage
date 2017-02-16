package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io._
import javax.imageio.ImageIO
import org.opencv.core.{CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}
import sun.misc.{BASE64Decoder, BASE64Encoder}

/**
  * Created by fang on 16-12-16.
  */
object SparkUtils {
  val imagePath = "file:///home/hadoop/ILSVRC2015/Data/CLS-LOC/train/n02113799"
  //val imagePath = "hdfs://218.199.92.225:9000/spark/n01491361"
  //val imagePath = "/home/fang/images/train/3"
  //hdfs dfs -rm -r /spark/kmeansModel
  val kmeansModelPath = "./spark/kmeansModel"
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

  //获取图像的sift特征,返回Mat
  def getImageSiftOfMat(image: Array[Byte]): Mat = {
    val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
    val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    test_mat.put(0, 0, data)
    val desc = new Mat
    val fd = FeatureDetector.create(FeatureDetector.SIFT)
    val mkp = new MatOfKeyPoint
    fd.detect(test_mat, mkp)
    val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
    //提取sift特征
    de.compute(test_mat, mkp, desc)
    desc
  }
  //获取图像的sift特征,返回Array[Byte]
  def getImageSift(image: Array[Byte]): Option[Array[Byte]] = {
    val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
    val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    test_mat.put(0, 0, data)
    val desc = new Mat
    val fd = FeatureDetector.create(FeatureDetector.SIFT)
    val mkp = new MatOfKeyPoint
    fd.detect(test_mat, mkp)
    val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
    //提取sift特征
    de.compute(test_mat, mkp, desc)
    test_mat.release()
    mkp.release()
    //判断是否有特征值
    if (desc.rows() != 0) {
      Some(Utils.serializeMat(desc))
    } else {
      None
    }
  }
  def deserializeArray(b: Array[Byte]): Array[Int] = {
    var data = null.asInstanceOf[Array[Int]]
    try {
      val in = new ObjectInputStream(new ByteArrayInputStream(b))
      data = in.readObject.asInstanceOf[Array[Int]]
      in.close()
    }
    catch {
      case cnfe: ClassNotFoundException => {
        cnfe.printStackTrace()
      }
      case ioe: IOException => {
        ioe.printStackTrace()
      }
    }
    data
  }

  /**
    * 对象转字节数组
    *
    * @param obj
    * @return
    */
  def ObjectToBytes(obj: Any): Array[Byte] = {
    var bytes = null.asInstanceOf[Array[Byte]]
    var bo = null.asInstanceOf[ByteArrayOutputStream]
    var oo = null.asInstanceOf[ObjectOutputStream]
    try {
      bo = new ByteArrayOutputStream()
      oo = new ObjectOutputStream(bo)
      oo.writeObject(obj)
      bytes = bo.toByteArray
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    bytes
  }

  /**
    * 字节数组转对象
    * @param bytes
    * @return
    */
  def BytesToObject(bytes: Array[Byte]): Object = {
    var obj = null.asInstanceOf[Object]
    var bi = null.asInstanceOf[ByteArrayInputStream]
    var oi = null.asInstanceOf[ObjectInputStream]
    try {
      bi = new ByteArrayInputStream(bytes)
      oi = new ObjectInputStream(bi)
      obj = oi.readObject
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    obj
  }

  //使用int，double都出错，改为float
  def byteArrToFloatArr(b: Array[Byte]): Array[Float] = {
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
}
