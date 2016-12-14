package com.fang.spark

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 16-11-15.
  */
object HBaseUpLoadImages {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseUpLoadImages").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val imagesRDD = sparkContext.binaryFiles("/home/fang/images")
  // val columnFaminlys :Array[String] = Array("image")
    //createTable(tableName,columnFaminlys,connection)
    imagesRDD.foreachPartition {
      iter => {
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
        val connection: Connection = ConnectionFactory.createConnection(hbaseConfig);
        val tableName = "imagesTest"
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        iter.foreach { imageFile =>
          val tempPath = imageFile._1.split("/")
          val len = tempPath.length
          val imageName = tempPath(len-1)
          val imageBinary:scala.Array[Byte]= imageFile._2.toArray()
          val put: Put = new Put(Bytes.toBytes(imageName))
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"),imageBinary)
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("path"), Bytes.toBytes(imageFile._1))
          table.put(put)
        }
        connection.close()
      }
    }

    sparkContext.stop()
  }

  def isExistTable(tableName: String, connection: Connection) {
    val admin: Admin = connection.getAdmin
    admin.tableExists(TableName.valueOf(tableName))
  }

  def createTable(tableName: String, columnFamilys: Array[String], connection: Connection): Unit = {
    val admin: Admin = connection.getAdmin
    if (admin.tableExists(TableName.valueOf(tableName))) {
      println("表" + tableName + "已经存在")
      return
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      admin.createTable(tableDesc)
      println("创建表成功")
    }
  }

  def addRow(tableName: String, row: Int, columnFaily: String, column: String, value: String, connection: Connection): Unit = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(Bytes.toBytes(row))
    put.addImmutable(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }
}
