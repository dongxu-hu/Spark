package com.atguigu.hostitem

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector



// 定义输入数据样例类
case class  UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 窗口聚合结果样例类
case class ItemViewCount(itemId:Long, count:Long, windowEnd: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    // 创建环境及配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换为样例类，提起时间戳设置watermark
    //val inputStream: DataStream[String] = env.readTextFile("F:\\MyWork\\GitDatabase\\flink-programing\\userBehavior\\src\\main\\resources\\UserBehavior.csv")


    // 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop202:9092")

    val inputStream: DataStream[String] =env.addSource( new FlinkKafkaConsumer[String]("hostitem",new SimpleStringSchema(),properties))


    val DataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
        .assignAscendingTimestamps(_.timestamp*1000)   // 获取watermark，时间戳提取器

    DataStream.print("data")

    // 获取窗口聚合结果
    val aggSteam :DataStream[ItemViewCount]= DataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1),Time.minutes(5))  // 窗口定义
      .aggregate(new ItemCountAgg(), new ItemCountWindowResult()) // 进行窗口聚合

    aggSteam.print("agg")

    // 安装窗口分组，排序输出TopN
    val resultStream: DataStream[String]= aggSteam
        .keyBy("windowEnd")
        .process(new TopNHostItemsResult(5))

    resultStream.print("res")

    env.execute("test")

  }
}

// 定义自定义的聚合函数    输入类型、聚合状态类型、 输出类型
class ItemCountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  // 每来一个元素，聚合状态加一
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b

}

// 实现自定义的窗口函数
class ItemCountWindowResult() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow,
                     input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    // 从元组中获取分组字段
    val itemId :Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd :Long= window.getEnd
    val count :Long = input.iterator.next()

    out.collect(ItemViewCount(itemId,count,windowEnd))

  }
}


// 实现自定义的keyedProcessFunction

class TopNHostItemsResult(n: Int) extends  KeyedProcessFunction[Tuple,ItemViewCount,String]{

  // 定义列表状态，用来保存当前窗口内所有商品的count结果
   private  var itemViewCountListState : ListState[ItemViewCount]=_


  override def open(parameters: Configuration): Unit ={
    itemViewCountListState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("ItemViewCount-list",classOf[ItemViewCount])
    )
  }


  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

    itemViewCountListState.add(value)
    // 注册一个定时器，windowEnd + 100 触发   为什么不需要进行判断？？
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  // 定时器触发，窗口的所有结果到齐，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple,
    ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 先将状态提取处理，使用list保存
    import scala.collection.JavaConversions._
    val allItemViewCountList : List[ItemViewCount] = itemViewCountListState.get().toList

    // 提前清空状态
    itemViewCountListState.clear()

    // 排序取TopN
    val topNHostItemViewCountList: List[ItemViewCount] = allItemViewCountList
      .sortBy(_.count)(Ordering.Long.reverse)
      .take(n)


    // 排名信息格式化输出
    val result : StringBuilder = new StringBuilder
    result.append("窗口结束时间").append(new Timestamp( timestamp -100)).append("\n")

    // 遍历topN列表，逐个输出

    for (i <- topNHostItemViewCountList.indices) {

      val currentItemViewCount :ItemViewCount = topNHostItemViewCountList(i)
      result.append("No.").append(i +1).append(":")
        .append(" \t 商品ID = ").append(currentItemViewCount.itemId)
        .append(" \t 热门度 = ").append(currentItemViewCount.count)
        .append("\n")

    }
    result.append("\n ----------------------\n\n ")

    // 控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())
  }
}