package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 定义输入数据样例类
case class  WebServerLogEvent(ip:String,useId:String,timestamp:Long,method:String,url:String)
// 窗口聚合结果样例类
case class PageViewCount(itemId:String, count:Long, windowEnd: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {

    // 创建环境及配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换为样例类，提起时间戳设置watermark
    val inputStream: DataStream[String] = env.readTextFile("F:\\MyWork\\GitDatabase\\flink-programing\\NewWorkFlowAnalysis\\src\\main\\resources\\apache.log")


    val DataStream: DataStream[WebServerLogEvent] = inputStream.map(line => {
      val arr: Array[String] = line.split(" ")
      val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp: Long = simpleDataFormat.parse(arr(3)).getTime
      WebServerLogEvent(arr(0),arr(1),timestamp,arr(5),arr(6))
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WebServerLogEvent](Time.seconds(3)) {
        override def extractTimestamp(element: WebServerLogEvent): Long = element.timestamp
      })


    // 开窗聚合
    val aggStream = DataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[WebServerLogEvent]("lataTag"))
      .aggregate(new PageContAgg(),new PageCountWindowResult())

    // 排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNPageResult(3))

    DataStream.print("data")
    resultStream.print("agg")
    aggStream.getSideOutput(new OutputTag[WebServerLogEvent]("lataTag")).print("lateTag")
    resultStream.print("res")
    env.execute("network flow job")
  }
}


class PageContAgg() extends AggregateFunction[WebServerLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: WebServerLogEvent, accumulator: Long): Long = accumulator +1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageCountWindowResult() extends  WindowFunction[Long,PageViewCount,String,TimeWindow]{

  override def apply(key: String, window: TimeWindow,
                     input: Iterable[Long], out: Collector[PageViewCount]): Unit = {

    out.collect(PageViewCount(key,input.head,window.getEnd))
  }
}


// 自定义ProcessFunction，进行排序输出
class TopNPageResult(n:Int) extends  KeyedProcessFunction[Long,PageViewCount,String]{

  // 定义一个列表状态
  lazy val pageViewCountListState:ListState[PageViewCount]= getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-ListState",classOf[PageViewCount]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageViewCountListState.add(value)

    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd +100)
    // 注册1min后定时器用于清理状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd +60*1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
    PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    if(timestamp == ctx.getCurrentKey + 60 *1000L){
      pageViewCountListState.clear()
      return
    }

    // 获取状态中的所有窗口聚合结果
    val allPageViewCounts :ListBuffer[PageViewCount]= ListBuffer()
    val iter: util.Iterator[PageViewCount] = pageViewCountListState.get().iterator()
    while (iter.hasNext){
      allPageViewCounts += iter.next()

      // 排序取topN
      val sortedPageViewCount = allPageViewCounts.sortWith(_.count > _.count).take(n)

      // 排名信息格式化
      // 排名信息格式化输出
      val result : StringBuilder = new StringBuilder
      result.append("窗口结束时间").append(new Timestamp( timestamp -100)).append("\n")

      // 遍历topN列表，逐个输出

      for (i <- sortedPageViewCount.indices) {
        val currentItemViewCount :PageViewCount = sortedPageViewCount(i)
        result.append("No.").append(i +1).append(":")
          .append(" \t 页面URL = ").append(currentItemViewCount.itemId)
          .append(" \t 热门度 = ").append(currentItemViewCount.count)
          .append("\n")
      }
      result.append("\n ----------------------\n\n ")

      // 控制输出频率
      Thread.sleep(1000)

      out.collect(result.toString())

    }
  }
}
