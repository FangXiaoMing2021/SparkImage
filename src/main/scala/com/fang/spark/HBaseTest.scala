/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.fang.spark

import org.apache.hadoop.hbase.client.{Admin, Connection, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark._
/**
  * create by fangfeikun on 2016.12.16
  * 测试HBase表中的数据
  */
object HBaseTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
      .setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    // please ensure HBASE_CONF_DIR is on classpath of spark driver
    // e.g: set it through spark.driver.extraClassPath property
    // in spark-defaults.conf or through --driver-class-path
    // command line option of spark-submit

    val hbaseConf = HBaseConfiguration.create()

    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "imagesTest")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("histogram"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)
    // Initialize hBase table if necessary
//    val connection = ConnectionFactory.createConnection(hbaseConf)
//    val admin = connection.getAdmin
//    if (!admin.tableExists(TableName.valueOf("imagesTable"))) {
//      val columnFamilys= List("image")
//      createTable("imagesTable",columnFamilys,connection)
//    }
    //读取HBase中表的数据
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    println(hBaseRDD.count())
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
//    hBaseRDD.toDF

    sc.stop()
    //admin.close()
  }
  //创建HBase表
  def createTable(tableName: String, columnFamilys: List[String], connection: Connection): Unit = {
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
  def isExistTable(tableName: String, connection: Connection) {
    val admin: Admin = connection.getAdmin
    admin.tableExists(TableName.valueOf(tableName))
  }
}
// scalastyle:on println
