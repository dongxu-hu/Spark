package com.atguigu.market_analysis



import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


// 恶意点击，增加黑名单机制
object AdStatisBYProvice2 {
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

    // 自定义过滤过程，将超出过滤上限的数据输出到测输出流
    val filterStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data =>(data.userId,data.adId))
      .process( new FilterBlackListUser(100))

    // 更加省份分组,开窗聚合
    val adCountStream = filterStream
      .keyBy(_.Provice)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new adCountAgg(),new adWindowFUnction())

    adCountStream.print()
    filterStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("black list")
    env.execute("ad click count job")

  }

}

// 定义输入输出样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)



class  FilterBlackListUser(n :Int) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{

  // 定义状态， 保存当前用户对当前广告的点击值
  lazy  val countState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
  lazy val resutlTimerTsState :ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resutlTimerTs-State",classOf[Long]))

  // 定义标识位，确认用户是否已经在黑名单
  lazy val isInBlackList : ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isInBlackList-State",classOf[Boolean]))

  override def processElement(value: AdClickLog,
                              ctx: KeyedProcessFunction[(Long, Long), AdClickLog,
                                AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

    // 获取count值
    val count: Long = countState.value()

    // 判断是否为当天第一条数据，如果是，增加第二天0点定时器
    if (count == 0){
      // 获取第二天零点时间
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) +1) *(1000*60*60*24)
      ctx.timerService().registerProcessingTimeTimer(ts)
      resutlTimerTsState.update(ts)
    }

    // 判断是否达到上限，加入黑名单
    if(count >= n){
      // 如果没有在黑名单中，那么输出到测输出流黑名单中
      if(!isInBlackList.value()){
        ctx.output(new OutputTag[BlackListWarning]("blacklist"),
          BlackListWarning(value.userId,value.adId,
          s"click over $n times today!!!"))

        isInBlackList.update(true)
      }
      return
    }
      out.collect(value)
    countState.update(count + 1)

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext,
                       out: Collector[AdClickLog]): Unit = {
    // 定时器触发时，判断是否为0点定时器，如果是，清空状态
    if(timestamp == resutlTimerTsState.value()){
      countState.clear()
      isInBlackList.clear()
      resutlTimerTsState.clear()
    }

  }
}



