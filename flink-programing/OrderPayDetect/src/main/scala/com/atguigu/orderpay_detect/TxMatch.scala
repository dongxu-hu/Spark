package com.atguigu.orderpay_detect


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 到账事件样例类
case class ReceiptlEvent(txId:String,PayChannel:String,timestamp:Long)


// 需求：  双流join，使用connect
object TxMatch {
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

    val ReceiptLogdataStream = ReceiptLog.map(line =>{
      val arr: Array[String] = line.split(",")
      ReceiptlEvent(arr(0),arr(1),arr(2).toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000L)


    // 使用双流join，一国两制,使用connect
    val resultStream = OrderLogdataStream.connect(ReceiptLogdataStream)
      .keyBy(_.txId,_.txId)
      .process(new TxMatchDetect())

    resultStream.print("res")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("OrderEvent")).print("OrderEvent")
    resultStream.getSideOutput(new OutputTag[ReceiptlEvent]("ReceiptlEvent")).print("ReceiptlEvent")
    env.execute("TxMatch job")

  }
}

class TxMatchDetect() extends KeyedCoProcessFunction[String,OrderEvent,ReceiptlEvent,(OrderEvent,ReceiptlEvent)]{

  // 定义状态，保存当前已到达的pay事件，和receipt事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-State",classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptlEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptlEvent]("receipt-State",classOf[ReceiptlEvent]))

  override def processElement1(pay: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent,
    ReceiptlEvent, (OrderEvent, ReceiptlEvent)]#Context, out: Collector[(OrderEvent, ReceiptlEvent)]): Unit = {

    // 来的是pay事件，判断当前是否已有receipt事件
    val receopts = receiptState.value()
    if(receopts != null){
      // 已有支付信息，正常输出
      out.collect((pay,receopts))
      // 清空状态,  payState为空不用清理
      receiptState.clear()
    } else{
      // 如果没有，注册定时器（等待5s），更新状态
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.timestamp*1000L + 5*1000L)
    }
  }

  override def processElement2(receipt: ReceiptlEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptlEvent,
    (OrderEvent, ReceiptlEvent)]#Context, out: Collector[(OrderEvent, ReceiptlEvent)]): Unit = {

    // 来的是receipt事件，判断当前是否已有pay事件
    val pays = payState.value()
    if(pays != null){
      // 已有支付信息，正常输出
      out.collect((pays,receipt))
      // 清空状态,  payState为空不用清理
      payState.clear()
    } else{
      // 如果没有，注册定时器（等待5s），更新状态
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer(receipt.timestamp*1000L + 5*1000L)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent,
    ReceiptlEvent, (OrderEvent, ReceiptlEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptlEvent)]): Unit = {

    // 判断是否有一个状态不为空，如果不为空，就是另一个没有来
    if(payState.value() !=null){
      ctx.output(new OutputTag[OrderEvent]("OrderEvent"),payState.value())
    }
    if(receiptState.value() !=null){
      ctx.output(new OutputTag[ReceiptlEvent]("ReceiptlEvent"),receiptState.value())
    }

    payState.clear()
    receiptState.clear()

  }
}
