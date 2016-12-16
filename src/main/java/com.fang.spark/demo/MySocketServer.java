package com.fang.spark.demo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Created by fang on 16-12-4.
 */
public class MySocketServer {
    public static final int PORT = 8888;

    public static void main(String args[]) throws IOException {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Started: " + serverSocket);
        try {
            //Blocks until a connection occurs:
            Socket socket = serverSocket.accept();
            try {
                System.out.println("Connection accepted: " + socket);

               // BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                FileReader fileReader = new FileReader("/home/fang/Downloads/apache_log.txt");
                BufferedReader br = new BufferedReader(fileReader);
                String tempString;
                while((tempString=br.readLine())!=null){
                    out.println(tempString);
                    Thread.sleep(100);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("closing……");
                socket.close();
            }


        } finally {
            serverSocket.close();
        }
    }
}

