package com.resoft.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object TableApiTestKafka {


  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.1c 创建老版本的流查询环境
    val settings:EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    //3.连接到kafka
    tableEnv.connect( new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("bootstrap.servers","192.168.226.101:9092")
        .property("zookeeper.connect","192.168.226.101:2181")
    )
        .withFormat(new Csv())
        .withSchema( new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
        ).createTemporaryTable("kafkaInputTable")

    /**
     * 需要将import org.apache.flink.table.api.scala.StreamTableEnvironment,修改成
     * * 需要将import org.apache.flink.table.api.scala._
     */
    val sensorTable : Table = tableEnv.from("kafkaInputTable") //将注册好的表提取出来
    sensorTable.toAppendStream[(String,Long,Double)].print()


    env.execute()
  }
}
