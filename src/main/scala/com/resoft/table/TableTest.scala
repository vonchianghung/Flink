package com.resoft.table

import com.resoft.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table,TableEnvironment}
import org.apache.flink.table.api.scala._

object TableTest {




    def main(args: Array[String]): Unit = {

      //1.创建执行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      var inputStream:DataStream[String] = env.readTextFile("C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\sensor.txt")

      val dataStream:DataStream[SensorReading] = inputStream
        .map( data => {//下面是一个匿名函数
          val dataArray = data.split(",")
          SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
        })

      //2.创建表执行环境
      val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)

      //3.基于数据流创建一张表,然后进行操作;
      val dataTable:Table = tableEnv.fromDataStream(dataStream)

      //4.1.调用table API,得到转换结果
      val resultTable:Table = dataTable
        .select("id,temperature")
        .filter("id == 'sensor_01'")

      //4.2.也可以转换成SQL得到转换的结果
      val resultSQL:Table = tableEnv.sqlQuery("select id,temperature from "+ dataTable+ " where id = 'sensor_01' ")


      //5.转换回数据流,打印输出
      val resultStream:DataStream[(String,Double)] = resultTable.toAppendStream[(String,Double)]
      val resultSQLStream:DataStream[(String,Double)] = resultSQL.toAppendStream[(String,Double)]

      resultTable.printSchema()
      resultStream.print("table API job")
      resultSQLStream.print("table SQL API job")

      env.execute()
    }


}
