package com.fang.spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by fang on 16-12-11.
 */
public class ParseLog {
    public static void main(String args[]) throws IOException{
        FileReader fileReader = new FileReader("/home/fang/Downloads/apache_log.txt");
        BufferedReader br = new BufferedReader(fileReader);
        String tempString;
        int i=0;
        while (i<10&&(tempString = br.readLine()) != null) {
            String[] parseStrs = tempString.split(" ");
            for(String str :parseStrs){
                System.out.println(str);
            }
            System.out.println("--------------------------");
            i++;
        }
    }
}
