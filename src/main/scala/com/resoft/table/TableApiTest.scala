package com.resoft.table

import com.resoft.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

object TableApiTest {


  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)

    //1.1c 创建老版本的流查询环境
    val settings:EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env,settings)


    val filePath = "C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\sensor.txt"
    //2.创建表执行环境


    //2.1测试从文件中读取数据
    tableEnv.connect(new FileSystem().path(filePath))
        .withFormat(new Csv())
        .withSchema(new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE()) )
        .createTemporaryTable("inputTbale") //注意pom文件中的flink-table-planner_2.11工具包一定要引入1.10.0,不能用1.9.1，后者布局被相关方

    //转换流打印输出
    /**
     * 需要将import org.apache.flink.table.api.scala.StreamTableEnvironment,修改成
     * * 需要将import org.apache.flink.table.api.scala._
     */
    val sensorTable : Table = tableEnv.from("inputTbale") //将注册好的表提取出来
    sensorTable.toAppendStream[(String,Long,Double)].print()

    //3.连接到kafka
//    tableEnv.connect( new Kafka()
//        .version("0.11")
//        .topic("sensor")
//        .property("bootstrap.servers","localhost:9092")
//        .property("zookeeper.connect","localhost:2181")
//    )
//        .withFormat(new Csv())
//        .withSchema( new Schema()
//            .field("id", DataTypes.STRING())
//            .field("timestamp", DataTypes.BIGINT())
//            .field("temperature", DataTypes.DOUBLE())
//
//        )

    env.execute()
  }
}
