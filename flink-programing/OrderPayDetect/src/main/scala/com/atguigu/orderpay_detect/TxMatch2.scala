package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{KeyedCoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector



// 需求：  双流join，使用intervaljoin   结果只能输出匹配上的数据
object TxMatch2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件读取数据
    val resource1 = getClass.getResource("/OrderLog.csv")
    val OrderLog = env.readTextFile(resource1.getPath)

    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val ReceiptLog = env.readTextFile(resource2.getPath)

    val OrderLogdataStream = OrderLog.map(line =>{
      val arr: Array[String] = line.split(",")
      OrderEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)

    val ReceiptLogdataStream = ReceiptLog.map(line =>{
      val arr: Array[String] = line.split(",")
      ReceiptlEvent(arr(0),arr(1),arr(2).toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .keyBy(_.txId)

    // 使用双流join，一国两制,使用connect
    val resultStream = OrderLogdataStream.intervalJoin(ReceiptLogdataStream)
      .between(Time.seconds(-3),Time.seconds(+5))
        .process(new TxMatchDetectWihtJoin())

    resultStream.print("res")
//    resultStream.getSideOutput(new OutputTag[OrderEvent]("OrderEvent")).print("OrderEvent")
//    resultStream.getSideOutput(new OutputTag[ReceiptlEvent]("ReceiptlEvent")).print("ReceiptlEvent")
    env.execute("TxMatch with join job")

  }
}

class TxMatchDetectWihtJoin() extends ProcessJoinFunction[OrderEvent,ReceiptlEvent,(OrderEvent,ReceiptlEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptlEvent,
                              ctx: ProcessJoinFunction[OrderEvent, ReceiptlEvent,
                                (OrderEvent, ReceiptlEvent)]#Context, out: Collector[(OrderEvent, ReceiptlEvent)]): Unit = {
    out.collect(left,right)

  }
}
