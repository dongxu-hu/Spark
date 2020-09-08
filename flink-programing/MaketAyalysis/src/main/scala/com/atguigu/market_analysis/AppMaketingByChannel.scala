package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.api.java.tuple.{Tuple,Tuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


// 输入数据的样例类
case class MarketingUerBehaior(userId:String,behavior:String,channel:String,timestamp:Long)

// 自定义测试数据源
class MarketingSimulatedEventSource extends RichSourceFunction[MarketingUerBehaior]{
  // 是否正常运行的标识位
  var running = true

  // 定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("ClICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "Wechat", "Weibo")

  override def run(ctx: SourceFunction.SourceContext[MarketingUerBehaior]): Unit = {

    // 定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L
    while (running && count < maxElements) {
      // 所有字段随机生成
      val id = UUID.randomUUID().toString
      val behaior = behaviorSet(Random.nextInt(behaviorSet.size))
      val channel = channelSet(Random.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUerBehaior(id,behaior,channel,ts))
      count +=1
      Thread.sleep(20)

    }
  }

  override def cancel(): Unit = false

}


// 定义输出类型统计样例类
case class MarketingViewCount(WindowStart: String, windowEnd:String,channel:String,behavior:String,count:Long)

object AppMaketingByChannel {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream:DataStream[MarketingUerBehaior] = env.addSource( new MarketingSimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    val resultStream:DataStream[MarketingViewCount] = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy("channel","behavior")      // 按照机型过滤卸载行为
      .timeWindow(Time.hours(1),Time.seconds(5)) //开滑动窗口进行统计
      .process( new MarketingCountByChannelFunction)

    resultStream.print()

    env.execute("APP marketing by channel job")
  }

}


class MarketingCountByChannelFunction() extends  ProcessWindowFunction[MarketingUerBehaior,MarketingViewCount,Tuple,TimeWindow]{
  override def process(key: Tuple, context: Context, elements: Iterable[MarketingUerBehaior], out: Collector[MarketingViewCount]): Unit = {

    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString

    val channel = key.asInstanceOf[Tuple2[String,String]].f0
    val behavior = key.asInstanceOf[Tuple2[String,String]].f1
    val count = elements.size

    out.collect(MarketingViewCount(start,end,channel,behavior,count))

  }
}
