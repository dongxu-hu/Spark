package com.atguigu.market_analysis

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AdStatisBYProvice {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换为样例类
    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map(line=>{
        val arr: Array[String] = line.split(",")
        AdClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp *1000)

    // 根据省份分组,开窗聚合
    val adCountStream = adLogStream
      .keyBy(_.Provice)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new adCountAgg(),new adWindowFUnction())

    adCountStream.print()
    env.execute("ad click count job")

  }

}

// 定义输入输出样例类
case  class  AdClickLog(userId:Long,adId:Long,Provice:String,city:String,timestamp:Long)
case class AdViewCountByProvince(windEnd:String,Provice:String,count:Long)


class  adCountAgg() extends  AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator +1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


class  adWindowFUnction() extends  WindowFunction[Long,AdViewCountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString

    out.collect(AdViewCountByProvince(end,key,input.head))

  }
}