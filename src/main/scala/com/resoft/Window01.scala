package com.resoft

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object Window01 {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    var inputDataStream:DataStream[String] = env.readTextFile("C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\sensor.txt")
    val inputDataStream:DataStream[String] = env.socketTextStream("192.168.226.101",7777)

    val resultStream:DataStream[SensorReading] = inputDataStream
      .map( data => {//下面是一个匿名函数
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
      .keyBy("id")
//      .window(EventTimeSessionWindows.withGap(Time.minutes(1)))//会话窗口
      .timeWindow(Time.seconds(15))
      .reduce(new MyReduce()) //这个是窗口聚合,取的是每个窗口中最小的温度

    //countWindow(10)每个10分钟统计一次

    inputDataStream.print("orig data")
    resultStream.print("result data")

    env.execute()

  }







  class MyReduce extends ReduceFunction[SensorReading]{
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
      //返回每个id的最大时间戳和最小温度
      SensorReading( value1.id,value1.timestamp.max(value2.timestamp),value1.temperature.min(value2.temperature))
    }
  }
}
