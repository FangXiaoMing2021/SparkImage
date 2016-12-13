package com.fang.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
//import java.util.NavigableMap;
/**
 * Created by hadoop on 16-11-24.
 */
public class HBaseTestCase {
    static Configuration cfg = HBaseConfiguration.create();
    //创建一张表，通过Admin HTableDescriptor 来创建
    public static void createTable(Connection connection,TableName tableName, String columnFamily)throws Exception{
        Admin admin = connection.getAdmin();
        if(admin.tableExists(tableName)){
            System.out.println("table Exists!");
        }else{
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
    }
    //添加一条数据，通过HTable Put 为已经存在的表来添加数据
    public static void put(Connection connection,TableName tableName,String row,String columnFamily,String column,String data)throws Exception{
        Table table = connection.getTable(tableName);
        Put p1 = new Put(Bytes.toBytes(row));
        p1.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
        table.put(p1);
    }
    //根据row key获取表中的该行数据
    public static void get(Connection connection,TableName tableName,String row)throws IOException {
        Table table = connection.getTable(tableName);
        Get g = new Get(Bytes.toBytes(row));
        Result result = table.get(g);
//        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
//        for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry:navigableMap.entrySet()){
//            System.out.print(Bytes.toString(entry.getKey())+"#");
//            NavigableMap<byte[], NavigableMap<Long, byte[]>> map =entry.getValue();
//            for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en:map.entrySet()){
//                System.out.print(Bytes.toString(en.getKey())+"##");
//                NavigableMap<Long, byte[]> ma = en.getValue();
//                for(Map.Entry<Long, byte[]>e: ma.entrySet()){
//                    System.out.print(e.getKey()+"###");
//                    System.out.println(Bytes.toString(e.getValue()));
//                }
//            }
//        }
        System.out.println("Get:"+result);
    }
    //根据TableName获取整张表中的数据
    public static void scan(Connection connection,TableName tableName)throws Exception{
        Table table = connection.getTable(tableName);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        //遍历打印表中的数据
//        for(Result r:rs){
//            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
//            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry:navigableMap.entrySet()){
//                System.out.print(Bytes.toString(r.getRow())+":");
//                System.out.print(Bytes.toString(entry.getKey())+"#");
//                NavigableMap<byte[], NavigableMap<Long, byte[]>> map =entry.getValue();
//                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en:map.entrySet()){
//                    System.out.print(Bytes.toString(en.getKey())+"##");
//                    NavigableMap<Long, byte[]> ma = en.getValue();
//                    for(Map.Entry<Long, byte[]>e: ma.entrySet()){
//                        System.out.print(e.getKey()+"###");
//                        System.out.println(Bytes.toString(e.getValue()));
//                    }
//                }
//            }
//            System.out.println();
//        }

    }
    //删除表中的数据
    public static boolean delete(Connection connection,TableName tableName) throws IOException{
        Admin admin = connection.getAdmin();
        if(admin.tableExists(tableName)){
            try{
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }catch(Exception ex){
                ex.printStackTrace();
                return false;
            }
        }
        return true;
    }
    public static void main(String args[]){
        TableName tableName = TableName.valueOf("hbase_tb");
        String columnFamily = "cf";
        try{
            cfg.set("hbase.zookeeper.property.clientPort", "2181");
            cfg.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu");
            Connection connection = ConnectionFactory.createConnection(cfg);
            HBaseTestCase.createTable(connection,tableName,columnFamily);
            HBaseTestCase.put(connection,tableName,"row1",columnFamily,"cl1","data");
            HBaseTestCase.put(connection,tableName,"row2",columnFamily,"cl1","fang");
            HBaseTestCase.put(connection,tableName,"row2",columnFamily,"cl2","fang");
            HBaseTestCase.put(connection,tableName,"row2",columnFamily,"cl3","fang");
            HBaseTestCase.put(connection,tableName,"row3",columnFamily,"cl4","fang");
            HBaseTestCase.get(connection,tableName,"row1");
            HBaseTestCase.scan(connection,tableName);
            if(true==HBaseTestCase.delete(connection,tableName)){
                System.out.println("Delete table:"+tableName.toString()+" success!");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
