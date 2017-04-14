package com.fang.spark;

import jodd.io.FileUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by fang on 17-4-13.
 */
public class ImageTest {
    public static void main(String args[]) throws IOException {
        File file = new File("/home/fang/images/2.JPEG");

        byte[] imageData = FileUtils.readFileToByteArray(file);
        FileUtil.writeBytes("/home/fang/images/copy.jpg",imageData);
        //BufferedImage bi = ImageIO.read(new ByteArrayInputStream(imageData));

    }
}
