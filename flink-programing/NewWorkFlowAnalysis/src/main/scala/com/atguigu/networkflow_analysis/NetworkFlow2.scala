package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 升级版， 1min为迟到时间，采用map收集数据进行处理。
object NetworkFlow2 {
  def main(args: Array[String]): Unit = {

    // 创建环境及配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换为样例类，提起时间戳设置watermark
//    val inputStream: DataStream[String] = env.readTextFile("F:\\MyWork\\GitDatabase\\flink-programing\\NewWorkFlowAnalysis\\src\\main\\resources\\apache.log")

    val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

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
      .aggregate(new PageContAgg2(),new PageCountWindowResult2())

    DataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[WebServerLogEvent]("lataTag")).print("lateTag")

    // 排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNPageResult2(3))

    resultStream.print()

    env.execute("network flow job")
  }
}


class PageContAgg2() extends AggregateFunction[WebServerLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: WebServerLogEvent, accumulator: Long): Long = accumulator +1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageCountWindowResult2() extends  WindowFunction[Long,PageViewCount,String,TimeWindow]{

  override def apply(key: String, window: TimeWindow,
                     input: Iterable[Long], out: Collector[PageViewCount]): Unit = {

    out.collect(PageViewCount(key,input.head,window.getEnd))
  }
}


// 自定义ProcessFunction，进行排序输出
class TopNPageResult2(n:Int) extends  KeyedProcessFunction[Long,PageViewCount,String]{

  // 为了对同一个key进行更新操作，定义映射状态
  lazy val pageViewCountmapState:MapState[String,Long]= getRuntimeContext
    .getMapState(new MapStateDescriptor[String,Long]("pageViewCount-mapState",classOf[String],classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageViewCountmapState.put(value.itemId,value.count)

    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd +100)
    // 注册1min后定时器用于清理状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd +60*1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
    PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    if(timestamp == ctx.getCurrentKey + 60 *1000L){
      pageViewCountmapState.clear()
      return
    }

    // 获取状态中的所有窗口聚合结果
    val allPageViewCounts :ListBuffer[(String,Long)]= ListBuffer()
    val iter: util.Iterator[Map.Entry[String, Long]] = pageViewCountmapState.entries().iterator()
    while (iter.hasNext){
      val value: Map.Entry[String, Long] = iter.next()
      allPageViewCounts.append((value.getKey,value.getValue))

      // 排序取topN
      val sortedPageViewCount = allPageViewCounts.sortWith(_._2 > _._2).take(n)

      // 排名信息格式化
      // 排名信息格式化输出
      val result : StringBuilder = new StringBuilder
      result.append("窗口结束时间").append(new Timestamp( timestamp -100)).append("\n")

      // 遍历topN列表，逐个输出
      for (i <- sortedPageViewCount.indices) {
        val currentItemViewCount :(String,Long) = sortedPageViewCount(i)
        result.append("No.").append(i +1).append(":")
          .append(" \t 页面URL = ").append(currentItemViewCount._1)
          .append(" \t 热门度 = ").append(currentItemViewCount._2)
          .append("\n")
      }
      result.append("\n ----------------------\n\n ")

      // 控制输出频率
      Thread.sleep(1000)

      out.collect(result.toString())

    }
  }
}
