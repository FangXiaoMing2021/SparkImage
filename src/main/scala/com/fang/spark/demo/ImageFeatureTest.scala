package com.fang.spark.demo

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.{ByteArrayInputStream, File}
import javax.imageio.ImageIO

import org.opencv.core.{Core, CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}
import org.opencv.imgproc.Imgproc

/**
  * Created by fang on 17-3-7.
  */
object ImageFeatureTest {
  def main(args: Array[String]): Unit = {
    val file = new File("/home/fang/images/train/3/n01751748_1554.JPEG")
    val bi: BufferedImage = ImageIO.read(file)
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    test_mat.put(0, 0, data)
    val desc = new Mat
    val siftFD = FeatureDetector.create(FeatureDetector.SIFT)
    val surfFD = FeatureDetector.create(FeatureDetector.SURF)
    val harrisFD = FeatureDetector.create(FeatureDetector.HARRIS)

    val mkp = new MatOfKeyPoint
    siftFD.detect(test_mat, mkp)
    val siftDE = DescriptorExtractor.create(DescriptorExtractor.SIFT)
    val surfDE = DescriptorExtractor.create(DescriptorExtractor.SURF)
    //val siftDE = DescriptorExtractor.create(DescriptorExtractor.SURF)
    siftDE.compute(test_mat, mkp, desc)
    println(desc.cols())
    println(desc.rows())
    surfFD.detect(test_mat, mkp)
    surfDE.compute(test_mat, mkp, desc)
    println(desc.cols())
    println(desc.rows())
    harrisFD.detect(test_mat, mkp)
    surfDE.compute(test_mat, mkp, desc)
    println(desc.cols())
    println(desc.rows())

//    val corners = new Mat
//    val tempDst = new Mat
//    // 找出角点
//    Imgproc.cornerHarris(test_mat, tempDst, 2, 3, 0.04)
//    // 归一化Harris角点的输出
//    val tempDstNorm = new Mat
//    Core.normalize(tempDst, tempDstNorm, 0, 255, Core.NORM_MINMAX)
//    Core.convertScaleAbs(tempDstNorm, corners)
//    println(corners.cols())
//    println(corners.rows())
//    println()
    //println(corners.dump())
  }
}
