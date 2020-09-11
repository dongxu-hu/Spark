package com.atguigu.orderpay_detect


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderPayNotwithCEP {
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


    // 直接自定义，检测不同的订单支付情况
    val orderResultStream = dataStream
      .keyBy(_.orderId)
      .process(new OrderPayProcessfunction())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("payed-but-timeout")).print("payed-but-timeout")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("data-not-found")).print("data-not-found")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("not-pay")).print("not-pay")


    env.execute("order pay timeout without CEP job")
  }
}

class  OrderPayProcessfunction() extends  KeyedProcessFunction[Long,OrderEvent,OrderPayResult]{

  // 定义状态， 用来保存之前是否create，pay事件
  lazy val isPayedState: ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-PayedState",classOf[Boolean]))
  lazy val isCreateedState: ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-CreateedState",classOf[Boolean]))
  lazy val timerState: ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("is-timerState",classOf[Long]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#Context, out: Collector[OrderPayResult]): Unit = {
    // 先拿到状态
    val ispayed = isPayedState.value()
    val isCreateed = isCreateedState.value()
    val timerts = timerState.value()

    // 判断当前事件的类型
    if(value.enventtype =="create"){
      // 1. 如果来的是create，要判断之前是否pay？
      if(ispayed){
        // 1.1 如果已经支付过，匹配成功，输出到主流
        out.collect(OrderPayResult(value.orderId,"pay successfully"))
        // 清空定时器，清空状态
        ctx.timerService().deleteEventTimeTimer(timerts)
        isCreateedState.clear()
        isPayedState.clear()
        timerState.clear()
      }else {
        //1.2 如果pay没有来过，注册定时器
        val ts = value.timestamp *1000 + 15*60*1000
        ctx.timerService().registerEventTimeTimer(ts)
        timerState.update(ts)
        isCreateedState.update(true)
      }

    }else if(value.enventtype =="pay"){
      // 2. 判断是否create？
      if(isCreateed){
        // 2.1 已经匹配成功,要判断支付时间是否超过支付时间
        if(value.timestamp *1000 < timerts){
          //2.1.1 没有超时，正常输出到主流
          out.collect(OrderPayResult(value.orderId,"pay successfully"))
        }else {
          //2.1.2 已经超时，因为是乱序数据，定时器没有触发，输出到侧输出流
          ctx.output(new OutputTag[OrderPayResult]("payed-but-timeout"),OrderPayResult(value.orderId,"payed-but-timeout"))
        }
        // 已经处理完，清空状态
        ctx.timerService().deleteEventTimeTimer(timerts)
        isCreateedState.clear()
        isPayedState.clear()
        timerState.clear()
      }else {
        //2.2  如果没有create， 乱序，注册定时器等待create
        ctx.timerService().registerEventTimeTimer(value.timestamp *1000L)

        // 更新状态
        timerState.update(value.timestamp *1000L)
        isPayedState.update(true)
      }
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#OnTimerContext, out: Collector[OrderPayResult]): Unit = {

    // 定时器判断， 说明create和pay 有一个没有来
    if(isPayedState.value()){
      // 如果pay过，说明create没有来
      ctx.output(new OutputTag[OrderPayResult]("data-not-found"),OrderPayResult(ctx.getCurrentKey,"data-not-found"))
    }else if(isCreateedState.value()){
      // 没有pay过，真正超时
      ctx.output(new OutputTag[OrderPayResult]("not-pay"),OrderPayResult(ctx.getCurrentKey,"not-pay"))
    }
    // 清空状态
    isCreateedState.clear()
    isPayedState.clear()
    timerState.clear()
  }

}
