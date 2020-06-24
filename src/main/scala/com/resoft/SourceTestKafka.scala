package com.resoft

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer011}

//输入数据的样例类


object SourceTestKafka {


  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //2.从文件中读数据 批处理和流处理的区别：批处理是将所有数据一次性全部读出来再出来,流式处理是一条一条的读数据
    //流式处理是数据读一条处理一条的,实际场景中处理的时候是从kafka中读取数据
//    var stream1:DataStream[String] = env.readTextFile("C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\sensor.txt")


    //4.从kafka中读数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.226.101:9092")
    properties.setProperty("group.id","consumer-group") //flink的消费者的group.id
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")
    val stream4 = env.addSource( new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))



    stream4.print()
    env.execute("stream4 job")
  }

}

