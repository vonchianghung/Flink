package com.resoft

import org.apache.flink.api.scala._

object WorkCountBatch {

  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件读取数据
    val inputDataSet:DataSet[String] = env.readTextFile("C:\\Users\\ThinkPad T480\\IdeaProjects\\Flink01\\src\\main\\resources\\work.txt")

    val resultDataSet:AggregateDataSet[(String,Int)]= inputDataSet
      .flatMap(_.split(" "))//
      .map((_, 1))//转换成一个二元组(word,count)
      .groupBy(0) //0 指的是以二元组第一个元素作为key分组
      .sum(1) //1 指的是聚合二元组中第2个元素的值

    /**
     * flatMap使用场景：将数据由少表动
     * 它的返回值类型要是一个可迭代类型
     * (冯XX,中国,安徽,池州)
     * * (冷雁冰,中国,江西,宜春)
     * *
     * * 将上面数据变成
     * * (冯XX,中国)
     * * (冯XX,中国,安徽)
     * * (冯XX,中国,安徽,池州)
     * * (冷雁冰,中国)
     * * (冷雁冰,中国,江西)
     * * (冷雁冰,中国,江西,宜春)
     * *
     */
    inputDataSet
        .flatMap(
          line => {
            val arr:Array[String] = line.split(",")//按,号对数据进行切分
            //把数据转入list
            List(
              (arr(0),arr(1)),
              (arr(0),arr(1),arr(2)),
              (arr(0),arr(1),arr(2),arr(3))
            )

          }
        )


    //打印输出
    resultDataSet.print()
  }

}
