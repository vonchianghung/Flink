package com.resoft

import org.apache.flink.streaming.api.scala._

//输入数据的样例类
case class SensorReading(id:String,timestamp:Long,temperature:Double)


object SourceTest {


  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //2.从文件中读数据 批处理和流处理的区别：批处理是将所有数据一次性全部读出来再出来,流式处理是一条一条的读数据
    //流式处理是数据读一条处理一条的,实际场景中处理的时候是从kafka中读取数据
    var stream1:DataStream[String] = env.readTextFile("C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\sensor.txt")


    val stream2:DataStream[SensorReading] = stream1
      .map( data => {//下面是一个匿名函数
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
//        .keyBy(0)
        .keyBy(data => data.id)
        .sum(2)//出来的结果很有意思,它是按条计算的.同一个keyby的值后面只会做累加,


    //分流操作
    val splitStream = stream2
        .split(data => {
          if(data.temperature > 32)
            Seq("hign")
          else {
            Seq("low")
          }
        })

    //定义一个高温的流
    val highTempStream:DataStream[SensorReading] = splitStream.select("hign")
    //定义一个低温的流
    val lowTempStream:DataStream[SensorReading] = splitStream.select("low")


//    highTempStream.print("high")
//    lowTempStream.print("low")


    //合流 先定义个三元组
    val warningStream:DataStream[(String,Double )] = highTempStream.map(
      data => (data.id,data.temperature )
    )

    val connectedStreams:ConnectedStreams[(String,Double ),SensorReading] = warningStream
      .connect(lowTempStream)


    val resultStream:DataStream[Object] = connectedStreams.map(
      warnData => ( warnData._1,warnData._2,"hign temp warning" ),
      lowTempStream => (lowTempStream.id,"normal") //lowTempStream是前面定义的变量
    )

    resultStream.print("connect stream")




    //3.打印数据
    //stream2.print("stream1")

    env.execute("stream1 job")
  }

}

