package com.resoft

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCountStream {

  def main(args: Array[String]): Unit = {

    //1.创建流处理执行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.接受socket文本流  --host 192.168.226.101 --port 7777
    var params:ParameterTool = ParameterTool.fromArgs(args)
    var hostname:String = params.get("host")
    var port:Int = params.getInt("port")
    val inputDataStream:DataStream[String] = env.socketTextStream(hostname,port)

    //3.定义转换操作 work count
    var resutlDataStream:DataStream[(String,Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)//不为空的过滤出来
      .map( (_,1))  //转换成(word,count)二元组
      .keyBy(0) //非常重要的算子,和goupby作用一样的
      .sum(1)


    resutlDataStream.print()
    //4.显示启动当前流式的处理程序
    env.execute("Stream work count")

  }
}
