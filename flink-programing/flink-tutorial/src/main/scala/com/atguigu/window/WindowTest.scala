package com.atguigu.window

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowTest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    environment.getConfig.setAutoWatermarkInterval(100)
    environment.setParallelism(1)

    // 设定使用事件时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 获取数据并进行相关转换
    val inputStream: DataStream[String] = environment.socketTextStream("hadoop202", 7777)

   /* val daStream: DataStream[SensorReading] = inputStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })



     //定义使用分配器
    val wmStream: DataStream[SensorReading] = daStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
    })

    // 定义使用时间窗口
    val result: DataStream[SensorReading] = daStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .timeWindow(Time.seconds(10))
      .minBy(2)

    daStream.print("data")
    result.print("result")*/

    // 老师代码
    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
    //  .assignAscendingTimestamps( data => data.timestamp * 1000L )    // 升序数据的时间戳提取
      //.assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
    //    override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    //  } )


    // 开窗聚合操作
    val aggStream = dataStream
      .keyBy(_.id)
      // 窗口分配器
      .timeWindow(Time.seconds(10))    // 10秒大小的滚动窗口
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late-data"))
      .reduce( new MyMaxTemp() )


    aggStream.getSideOutput(new OutputTag[SensorReading]("late-data")).print("late")

    dataStream.print("date")
    aggStream.print("agg")

    environment.execute()

  }

}

// 自定义取窗口最大温度值的聚合函数
class MyMaxTemp() extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp + 1, value1.temperature.max(value2.temperature))
}