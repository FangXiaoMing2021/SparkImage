package com.fang.spark.demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *  * 关于使用
 * Created by Administrator on 2016/11/2.
 */
public class FFmpegUtilTest {

    //cmd:
    //c:\ffmpeg -i c:\abc.mp4 e:\sample.jpg -ss 00:00:05  -r 1 -vframes 1  -an -vcodec mjpeg
    public void makeScreenCut(String ffmepgPath,String videoRealPath,String imageRealName){
        List<String> commend = new ArrayList<String>();
        commend.add(ffmepgPath);
        commend.add("-i");
        commend.add(videoRealPath);
        commend.add("-y");
        commend.add("-f");
        commend.add("image2");
        commend.add("-ss");
        commend.add("8");
        commend.add("-t");
        commend.add("0.001");
        commend.add(imageRealName);

        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command(commend);
            builder.redirectErrorStream(true);
            System.out.println("视频截图开始...");

            Process process = builder.start();
            InputStream in = process.getInputStream();
            byte[] bytes = new byte[1024];
            System.out.print("正在进行截图，请稍候");
            while (in.read(bytes)!= -1){
                System.out.println(".");
            }
            System.out.println("");
            System.out.println("视频截取完成...");

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("视频截图失败！");
        }
    }
}