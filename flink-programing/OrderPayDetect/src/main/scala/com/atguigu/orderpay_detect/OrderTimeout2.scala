package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 实现的结果是：   支付失败的进行输出

object OrderTimeout2 {
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

    // 3. 定义一个侧输出流标签，用于将超时事件输出
    val timeoutputTag = new OutputTag[OrderPayResult]("timeout")

    // 4. 检出复杂事件，并转换输出结果
    val resultStream = patternSteam.select(timeoutputTag,new OrderpayTimeoutSelect(),new OrderpaySelect2())

    resultStream.getSideOutput(timeoutputTag).print("fail")
    resultStream.print("success")

    env.execute("order pay timeout jod")

  }
}

class OrderpaySelect2() extends PatternSelectFunction[OrderEvent,OrderPayResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {

    val orderId: Long = map.get("pay").iterator().next().orderId
    OrderPayResult(orderId,"pay succssfully")
  }
}

class OrderpayTimeoutSelect() extends  PatternTimeoutFunction[OrderEvent,OrderPayResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderPayResult = {

    val timeoutorderId: Long = map.get("create").iterator().next().orderId

    OrderPayResult(timeoutorderId,s"pay fail at $l")
  }
}