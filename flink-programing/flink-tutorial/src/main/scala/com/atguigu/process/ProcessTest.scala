package com.atguigu.process

import java.util

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessTest {

  def main(args: Array[String]): Unit = {
    //  环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

    val dateStream: DataStream[SensorReading] = inputStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })


    //process function 实例

    val processstream: DataStream[Int] = dateStream.keyBy("id")
      .process(new MykeyedProcessfunction)

    processstream.getSideOutput(new OutputTag[Int]("silde")).print()

    env.execute("process function test")

  }
}

// 自定义keyed Processfunction
class MykeyedProcessfunction extends KeyedProcessFunction[Tuple,SensorReading,Int]{

  var myState: ListState[Int]=_


  override def open(parameters: Configuration): Unit = {
    myState= getRuntimeContext.getListState(new ListStateDescriptor[Int]("my-state",classOf[Int]))
  }

  def processElement(i: SensorReading,
                              context:KeyedProcessFunction[Tuple, SensorReading, Int]#Context,
                              collector: Collector[Int]): Unit = {
    myState.get()
    myState.add(10)
    myState.update(new util.ArrayList[Int]())

    // 侧输出流
    context.output(new OutputTag[Int]("side"),10)
    // 获取key
    context.getCurrentKey

    // 获取时间相关
    context.timestamp()
    context.timerService().currentWatermark()

    // 定义定时器，10ms后进行
    context.timerService().registerProcessingTimeTimer(context.timerService()
      .currentProcessingTime()+10)

  }

  // 定时器触发时的操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple,
    SensorReading, Int]#OnTimerContext, out: Collector[Int]): Unit =
    {
      println("timer occur")
      ctx.output(new OutputTag[Int]("side"),15)
      out.collect(23)
    }

  override def close(): Unit = super.close()

}