package com.fang.spark;

import kafka.utils.VerifiableProperties;

/**
 * Created by fang on 17-2-13.
 */
public class ObjectEncoder implements kafka.serializer.Encoder<ImageMember>{
    public ObjectEncoder(){}
    public ObjectEncoder(VerifiableProperties verifiableProperties){
    }
   //@Override
    public byte[] toBytes(ImageMember imageMember) { //填写你需要传输的对象

        //return Utils.ObjectToBytes(imageMember);
        return null;
    }
}