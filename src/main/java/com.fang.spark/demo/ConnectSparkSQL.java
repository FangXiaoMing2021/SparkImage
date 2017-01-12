package com.fang.spark.demo;

import java.sql.*;

/**
 * Created by fang on 17-1-8.
 */
public class ConnectSparkSQL {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:hive2://fang-ubuntu:10000/";
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(url,"hive","123456");
    /*    try {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, null, null,
                    new String[] { "table" });
            while (rs.next()) {
                System.out.println("表名：" + rs.getString(3));
                System.out.println("表所属用户名：" + rs.getString(2));
                System.out.println("------------------------------");
            }
            conn.close();
        } catch (Exception e) {
            try {
                conn.close();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/

        Statement stmt = conn.createStatement();
        String sql = "select * from info";
        System.out.println("Running"+sql);
        ResultSet res = stmt.executeQuery(sql);
        while(res.next()){
            System.out.println("id: "+res.getInt(1)+"\tname: "+res.getString(2)+"\tage: "+res.getString(3));
        }
    }
}
