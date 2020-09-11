package com.atguigu.orderpay_detect


import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 实现的结果是：   支付成功的进行输出

// 定义输入输出样例类
case class OrderEvent(orderId:Long,enventtype:String,txId:String,timestamp:Long)
case class OrderPayResult(orderId:Long,resultMsg:String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val inputStream = env.readTextFile(resource.getPath)
    val dataStream = inputStream.map(line =>{
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000L)


    // 1. 定义一个匹配模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.enventtype =="create")
      .followedBy("pay").where(_.enventtype =="pay")
      .within(Time.minutes(15))

    // 2. 将模式应用到数据流中
    val patternSteam = CEP.pattern(dataStream.keyBy(_.orderId),orderPayPattern)

    // 3. 检出复杂事件，并转换输出结果
    val resultStream = patternSteam.select(new OrderpaySelect())

    resultStream.print()

    env.execute("order pay timeout jod")

  }
}

class OrderpaySelect() extends PatternSelectFunction[OrderEvent,OrderPayResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {
    val orderId: Long = map.get("pay").iterator().next().orderId
    OrderPayResult(orderId,"pay succssfully")
  }
}
