package com.atguigu.networkflow_analysis


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 输出数据样例类
case class  UvCount(windwoEnd:Long,count: Long)
object UniqueVistirot {
  def main(args: Array[String]): Unit = {

    // 创建环境及配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换为样例类，提起时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")    // 引用当前路径下路径
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val DataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000)   // 获取watermark，时间戳提取器，升序获取的watermark比正常时间戳晚 1ms


    val uvStream = DataStream
      .filter(_.behavior =="pv")
      .timeWindowAll(Time.hours(1))  //全部数据直接开窗，所有数据在同一组
//      .apply( new UvCountResult())  // 先收集所有数据，在遍历放入到set集合
        .aggregate(new UvCountAgg(), new UvCountAggResult())

    uvStream.print("uv")
    env.execute("uv test")
  }
}


// 实现自定义的windowFunction
class  UvCountResult() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{

  override def apply(window: TimeWindow,
                     input: Iterable[UserBehavior],
                     out: Collector[UvCount]): Unit = {
    // 使用set集合类型来保存所有的userid，自动去重
    var idset = Set[Long]()
    for (userbehavior <- input) {
      idset += userbehavior.userId

    }

    // 输出set 的大小
    out.collect(UvCount(window.getEnd,idset.size))
  }
}

class UvCountAgg() extends  AggregateFunction[UserBehavior,Set[Long],Long]{
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

class UvCountAggResult() extends  AllWindowFunction[Long,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect(UvCount(window.getEnd,input.head))
  }
}
