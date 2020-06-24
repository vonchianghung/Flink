package com.resoft.state

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 需求：温度变化报警,
 *
 * flatMap的时候,可以使用.flatMap(new TempChangeWarningWithFlatMap(10.0) )方法来调用
 * 也可以用.flatMapWithState方法、在这里面做一个对初始state的处理,因为上面的flatMap调用TempChangeWarningWithFlatMap方法时、
 * 第一次state是空的,
 */

//输入数据的样例类
case class SensorReading(id:String,timestamp:Long,temperature:Double)

object StateTest {

  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //手动开启checkpoint，并指定时间间隔,间隔参数是以毫秒为单位的:1 秒=1000 毫秒
    env.enableCheckpointing(1000L)
    //设置超时时间,如果超时时间超过ck的间隔,会村长下面情况：前面一个ck还没保存完，第二ck已经开启了,这时候你是否允许一个ck没做完、另外一个ck
    //可以开启了,flink是允许的,需要你指定同时允许几个
    env.getCheckpointConfig.setCheckpointTimeout(3000L)
    //允许你同时做两个checkpoint的,
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //表示两个checkpoint之间要留出多少的间隔用于处理flink的数据
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    //任务挂了以后要不要自动从上一次checkpoint恢复
    env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
    //允许当前checkpoint能失败几次,
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    val inputStream = env.socketTextStream("192.168.226.101",7777)
    var dataStream:DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      } )

    val warningStream:DataStream[(String,Double,Double)] = dataStream
      .keyBy("id")
//        .flatMap(new TempChangeWarningWithFlatMap(10.0) )

        .flatMapWithState[(String,Double,Double),Double]({
          case (inputData:SensorReading,None) => (List.empty,Some(inputData.temperature))
          case (inputData:SensorReading,lastTemp:Some[Double]) => {
            val diff = (inputData.temperature - lastTemp.get).abs
            if ( diff > 10.0 ){
              ( List( (inputData.id,lastTemp.get,inputData.temperature)),Some(inputData.temperature) )
            } else {
              (List.empty,Some(inputData.temperature))
            }
          }

        })


    dataStream.keyBy("id").print("dataStream.keyBy打印")

    warningStream.print()

    env.execute("state job")
  }

}


class TempChangeWarning(threshold:Double) extends RichMapFunction[SensorReading,(String,Double,Double)]{

  //定义状态变量,上一次的温度值,初始化值为null
  private var lastTempState:ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
  }

  override def map(in: SensorReading): (String, Double, Double) = {
    //从状态中取出上一次状态的值
    val lastTemp = lastTempState.value()

    //你取出了上一次的状态值,还要把最新状态的值更新进去
    lastTempState.update(in.temperature)

    //跟当前温度值计算差值,然后跟阈值比较,如果大于就报警
    val diff = (in.temperature - lastTemp).abs

    if(diff > threshold){
      (in.id,lastTemp,in.temperature)//差值大于阈值的话、
    }else
      null

  }
}

//自定义RichFlatMapFunction，可以输出多个结果
class TempChangeWarningWithFlatMap(threshold:Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{

  lazy val lastTempState = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("last-temp",classOf[Double]))

  //通过flatMap中的out函数可以做输出
  override def flatMap(in: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //从状态中取出上一次状态的值
    val lastTemp = lastTempState.value()
    //你取出了上一次的状态值,还要把最新状态的值更新进去
    lastTempState.update(in.temperature)
    //跟当前温度值计算差值,然后跟阈值比较,如果大于就报警
    val diff = (in.temperature - lastTemp).abs

    if(diff > threshold){
      out.collect( (in.id,lastTemp,in.temperature)  )//差值大于阈值的话、
    }
  }
}
