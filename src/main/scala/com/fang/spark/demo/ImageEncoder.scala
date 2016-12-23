package com.fang.spark.demo

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties

/**
  * Created by fang on 16-12-22.
  */
class ImageEncoder[T](props:VerifiableProperties)extends Encoder[T]{
  override def toBytes(t: T) :Array[Byte]= {
    if(t==null){
      null
    }else{
      var bo:ByteArrayOutputStream = null
      var oo:ObjectOutputStream = null
      var byte:Array[Byte] = null
      try{
        bo = new ByteArrayOutputStream()
        oo = new ObjectOutputStream(bo)
        oo.writeObject(t)
        byte = bo.toByteArray
      }catch{
        case e:Exception =>  byte
      }finally{
        bo.close()
        oo.close()
      }
      byte
    }
  }

}
