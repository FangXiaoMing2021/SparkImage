package com.fang.spark.demo

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/**
  * Created by fang on 16-12-22.
  */
class ImageDecoder(props:VerifiableProperties) extends Decoder[BufferedImage] {
  override def fromBytes(bytes: Array[Byte]):BufferedImage = {
    if(bytes==null){
      null
    }else{
      var bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(bytes))
      bi
    }
//    var t:T = null.asInstanceOf[T]
//    var bi:ByteArrayInputStream = null
//    var oi:ObjectInputStream = null
//    try{
//      bi = new ByteArrayInputStream(bytes)
//      oi = new ObjectInputStream(bi)
//      t = oi.readObject().asInstanceOf[T]
//    }catch{
//      case e:Exception => {
//        e.printStackTrace()
//        null.asInstanceOf[T]
//      }
//    }finally {
//      bi.close()
//      oi.close()
//    }
//    t
  }
}
