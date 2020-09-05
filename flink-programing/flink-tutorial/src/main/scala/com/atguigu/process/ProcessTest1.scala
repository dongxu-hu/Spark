package com.atguigu.process

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessTest1 {

  def main(args: Array[String]): Unit = {
    //  环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

    val dateStream: DataStream[SensorReading] = inputStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)    })

    // 需求： 检测10秒钟内温度是否连续上升，如果上升，那么报警
//    val warnStram = dateStream
//        .keyBy(0)
//        .process( new TemIncreaWarning(10000L))
val warnStram: DataStream[String] = dateStream
  .keyBy(0)
  .process(new TemIncreaWarning(10000L))

    warnStram.print()
    env.execute("process function test")
  }
}

// 自定义keyed process function 实现10秒内温度连续上升报警检测
class TemIncreaWarning(time: Long) extends KeyedProcessFunction[Tuple,SensorReading,String]{

  // 首先定义状态
  lazy val lastTemStar : ValueState[Double]=getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("last-tem",classOf[Double]))

  // 定义当前定时器时间戳状态
  lazy val curTimeState : ValueState[Long]=getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("current-tem",classOf[Long]))

  override def processElement(i: SensorReading,context: KeyedProcessFunction[Tuple, SensorReading, String]#Context,
                              collector: Collector[String]): Unit = {

    // 取出状态
    val lasttmp = lastTemStar.value()
    var curtmp = curTimeState.value()
    lastTemStar.update(i.temperature)

    // 如果温度上升，并且没有定时器，注册一个10秒后的定时器
    if(i.temperature > lasttmp && curtmp == 0){
      val ts = context.timerService().currentProcessingTime() + time
      context.timerService().registerProcessingTimeTimer(ts)
      curTimeState.update(ts)
    } else if(i.temperature < lasttmp){
      // 如果温度下降，那么删除定时器
      context.timerService().deleteProcessingTimeTimer(curtmp)
      //清空状态
      curTimeState.clear()

    }
  }
  //设置定时器
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 定时器触发，说明10秒内没有温度下降
    out.collect(s"传感器${ctx.getCurrentKey} 的温度值已经连续 ${time/1000}秒上升了")
    // 清空定时器时间戳状态
    curTimeState.clear()
  }
}

