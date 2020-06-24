package com.resoft.table

import com.resoft.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

object TableApiTime {


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


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.从文件中读数据:数据读取完成之后,是一行的记录
    var inputDataStream:DataStream[String] = env.readTextFile("C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\sensor.txt")

    //这一步是映射成样例类的数据流
    val dataStream:DataStream[SensorReading] = inputDataStream
      .map( data => {//下面是一个匿名函数
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })


    //3.将dataStream转换成Table，
    //追加process Time字段
    val sensorTable : Table = tableEnv.fromDataStream(dataStream,'id,'timestamp as 'ts,'temperature, 'pt.proctime)

    //追加event time字段
    val sensorTableET : Table = tableEnv.fromDataStream(dataStream,'id,'timestamp as 'ts,'temperature, 'et.rowtime)

    sensorTable.printSchema()
    sensorTable.toAppendStream[Row].print("Row形式")

    env.execute()
  }
}
