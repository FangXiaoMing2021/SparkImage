package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io._
import javax.imageio.ImageIO

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.opencv.core._
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}
import org.opencv.imgproc.Imgproc
import sun.misc.{BASE64Decoder, BASE64Encoder}

/**
  * Created by fang on 16-12-16.
  */
object ImagesUtil {
  //n01491361  n01984695
  //val imagePath = "file:///home/hadoop/ILSVRC2015/Data/CLS-LOC/train/n01491361"
  val imagePath = "hdfs://202.114.30.171:9000/imagesNet/n01530575"
  //val imagePath = "/home/fang/images/n01984695"n01491361
  //hdfs dfs -rm -r /spark/kmeansModel
  //val imagePath = "/home/fang/imageTest"
  //Exception in thread "main" org.apache.hadoop.security.AccessControlException:
  // Permission denied: user=user, access=WRITE, inode="/kmeansModel/metadata/_temporary/0":hadoop:supergroup:drwxr-xr-x

  val kmeansModelPath = "hdfs://202.114.30.171:9000/user/kmeansModel"
  private[spark] val encoder = new BASE64Encoder
  private[spark] val decoder = new BASE64Decoder
  //val imageTableName = "imageNetTable"
  val imageTableName = "imagesTest"

  def loadHBaseConf(): Configuration ={
    val hbaseConf = HBaseConfiguration.create()
    // Caused by: java.lang.IllegalArgumentException: KeyValue size too large
    //设置HBase中表字段最大大小
    hbaseConf.set("hbase.client.keyvalue.maxsize","524288000");//最大500m
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    hbaseConf
  }

  def loadSparkConf(appName:String): SparkConf ={
    val sparkConf = new SparkConf()
      .setAppName(appName)
      //.setMaster("local[4]")
      //.setMaster("spark://fang-ubuntu:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf
  }
  /**
    * 使用int，double都出错，改为float
    * 将byte[]数组反序列化为float[]
    * @param b
    * @return
    */
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


  /**
    * java.lang.UnsupportedOperationException: Mat data type is not compatible: 0
    * 没有进行异常处理，出现上面错误，原因是没有提取到特征值，mat为空
    * 将Mat序列化为byte[]
    * @param mat
    * @return
    */
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


  /**
    *
    * @param beginTime
    * @param message
    */
  def printComputeTime(beginTime: Long, message: String) = {
    println("***************************************************************")
    println(message + " 耗时: " + (System.currentTimeMillis() - beginTime) + "ms")
    println("***************************************************************")
  }

  /**
    * 获取图像的sift特征,返回Mat
    * @param image
    * @return
    */
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

  /**
    * 获取图像的sift特征,返回Array[Byte]
    *
    * @param image 传入参数是图像字节数组
    * @return
    */

  def getImageSift(image: Array[Byte]): Option[Array[Byte]] = {
    val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
    //should be multiple of the Mat channels count
    val numComponents = bi.getColorModel.getNumColorComponents
    var test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
    if (numComponents == 3) {
     test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
    } else if(numComponents==1){
      test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    }else{
      println("**********************Components not 1 and 3****************************")
      return None
    }
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    test_mat.put(0, 0, data)
    val desc = new Mat
    val fd = FeatureDetector.create(FeatureDetector.SIFT)
    /*
      结合surf和harris
     */
    //val fd1 = FeatureDetector.create(FeatureDetector.SURF)
    //val fd = FeatureDetector.create(FeatureDetector.HARRIS)
    //OpenCV Error: Sizes of input arguments do not match
    val mkp = new MatOfKeyPoint
    fd.detect(test_mat, mkp)
    val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
    //val de1 = DescriptorExtractor.create(DescriptorExtractor.SURF)
    //提取sift特征
    de.compute(test_mat, mkp, desc)
    test_mat.release()
    mkp.release()
    if (desc.rows() != 0) {
      Some(Utils.serializeMat(desc))
    } else {
      println("**********************No Sift Feature****************************")
      None
    }
    //desc.push_back()

    //    val dstMat:Mat = desc.column(4);             //M为目的矩阵 3*4
    //    srcMat.copyTo(dstMat);
  }

  /**
    * 传入图像字节数组,获取Harris特征点
    * 根据Harris特征点获取SIFT描述
    * 返回字节数组
    *
    * @param image :Array[Byte]
    * @return Array[Byte]
    */
  //  @throws[IllegalArgumentException]
  //  @throws[CvException]
  def getImageHARRIS(image: Array[Byte]): Option[Array[Byte]] = {
    try {
      val bi = ImageIO.read(new ByteArrayInputStream(image))
      //should be multiple of the Mat channels count
      val numComponents = bi.getColorModel.getNumColorComponents
      var test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
      if (numComponents == 3) {
        test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
      } else if(numComponents==1){
        test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
      }else{
        println("bi.getColorModel.getNumComponents " + bi.getColorModel.getNumComponents);
        return None
      }
      // java.lang.IllegalArgumentException:
      // Numbers of source Raster bands and source color space components do not match
      val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
      test_mat.put(0, 0, data)
      val desc = new Mat
      val fd = FeatureDetector.create(FeatureDetector.HARRIS)
      val mkp = new MatOfKeyPoint
      fd.detect(test_mat, mkp)
      val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
      de.compute(test_mat, mkp, desc)
      test_mat.release()
      mkp.release()
      if (desc.rows() != 0) {
        Some(Utils.serializeMat(desc))
      } else {
        println("************************No Harris Feature*********************************")
        None
      }

    } catch {
      case iae: IllegalArgumentException =>
        println(iae.getMessage)
        None
      case cve: CvException =>
        println(cve.getMessage)
        None
      case _: Exception =>
        println("have a Exception in getImageHARRIS")
        None
    }
  }

  /**
    *
    * @param b
    * @return
    */
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
    *
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

  /**
    * 使用int，double都出错，改为float
    * @param b
    * @return
    */
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



  /**
    * 输入从HBase中读取的sift和harris特征值
    * 将特征值转换为二维数组
    * @param siftByte
    * @param harrisByte
    * @return
    */
  def featureArr2TowDim(siftByte:Array[Byte],harrisByte:Array[Byte]):Array[Array[Float]]={
    val siftArray: Array[Float] = Utils.deserializeMat(siftByte)
    val harrisArray: Array[Float] = Utils.deserializeMat(harrisByte)
    val siftSize = siftArray.length / 128
    val harrisSize = harrisArray.length / 128
    val featureTwoDim = new Array[Array[Float]](siftSize+harrisSize)
    //添加sift
    for (i <- 0 to siftSize - 1) {
      val xs: Array[Float] = new Array[Float](128)
      for (j <- 0 to 127) {
        xs(j) = siftByte(i * 128 + j)
      }
      featureTwoDim(i) = xs
    }
    //添加harris
    for (i <- 0 to harrisSize - 1) {
      val xs: Array[Float] = new Array[Float](128)
      for (j <- 0 to 127) {
        xs(j) = harrisByte(i * 128 + j)
      }
      featureTwoDim(i+siftSize) = xs
    }
    featureTwoDim
  }

  /**
    * 将HBase中的sift特征数组转换为二维数据
    * @param siftByte
    * @return
    */
  def siftArr2TowDim(siftByte: Array[Byte]): Array[Array[Float]] = {
    //    val siftByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("sift"))
    val siftArray: Array[Float] = Utils.deserializeMat(siftByte)
    val size = siftArray.length / 128
    val siftTwoDim = new Array[Array[Float]](size)
    for (i <- 0 to size - 1) {
      val xs: Array[Float] = new Array[Float](128)
      for (j <- 0 to 127) {
        xs(j) = siftByte(i * 128 + j)
      }
      siftTwoDim(i) = xs
    }
    siftTwoDim
  }

  /**
    *
    * @param roi
    */
  def transformOneToRGBA(roi: Mat): Unit = {
    val mBGR = new Mat()
    Imgproc.cvtColor(roi, mBGR, Imgproc.COLOR_RGBA2BGR, 0)
    val threshBin = new Mat()
    Core.inRange(mBGR, new Scalar(0, 255, 255), new Scalar(0, 255, 255), threshBin)

    // ok. now we got an 8bit binary img in threshBin,
    // convert it to rbga:
    val threshRGBA = new Mat()
    Imgproc.cvtColor(threshBin, threshRGBA, Imgproc.COLOR_GRAY2RGBA)
    // now we can put it back into it's old place:
    threshRGBA.copyTo(roi)
  }

  /**
    *
    * @param file
    * @return
    */
  def getImageToString(file: File): String = {
    var imageString: String = ""
    try {
      val bi = ImageIO.read(file)
      val baos = new ByteArrayOutputStream
      ImageIO.write(bi, "jpg", baos)
      val bytes = baos.toByteArray
      imageString = encoder.encodeBuffer(bytes).trim
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
    imageString
  }

  /**
    *
    * @param base64String
    * @param fileName
    */
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
