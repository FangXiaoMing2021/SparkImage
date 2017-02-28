package com.fang.spark.demo;

import java.io.Serializable;

/**
 * Created by fang on 17-2-13.
 */
public class ImageMember implements Serializable {
    private String imageName;
    private byte[] image;

    public ImageMember(String imageName, byte[] image) {
        this.imageName = imageName;
        this.image = image;
    }

    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    public byte[] getImage() {
        return image;
    }

    public void setImage(byte[] image) {
        this.image = image;
    }
}
