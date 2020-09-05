package com.atguigu.process

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//需求 连续10秒钟温度上升报警
object ProcessExec {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val inputDataStream: DataStream[String] = environment.socketTextStream("hadoop202", 7777)

    val dataStream: DataStream[SensorReading] = inputDataStream.map(data => {
      val split: Array[String] = data.split(",")
      SensorReading(split(0).trim, 1L, split(1).trim.toDouble)
    })



    val resultDS: DataStream[String] = dataStream
      .keyBy(0)
      .process(new TemIncreaWarning(10000L))

    dataStream.print("data")
    resultDS.print("arr")
    environment.execute()
  }

}

class MyProcessfunction(timestamp: Long) extends KeyedProcessFunction[String,SensorReading,String]{

  // 设置 状态，即后面当前时间、 传入温度
  lazy val lastTem: ValueState[Double]= getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-tem",classOf[Double]))
  lazy val current : ValueState[Long]= getRuntimeContext.getState(new ValueStateDescriptor[Long]("current",classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {

    // 获取状态
    val lastt: Double = lastTem.value()
    val currtime: Long = current.value()
    lastTem.update(value.temperature)

    if(lastt > value.temperature && currtime ==0){
      val ts: Long = ctx.timerService().currentProcessingTime() + currtime
      ctx.timerService().registerProcessingTimeTimer(ts)
      current.update(ts)
    }else if(lastt < value.temperature){
      // 删除定时器
      ctx.timerService().deleteProcessingTimeTimer(currtime)
      current.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    // 输出报警结果
    out.collect(s"${ctx.getCurrentKey} 已连续${timestamp/1000}s温度上升，警报警报！！")
    current.clear()
  }

}