package com.atguigu.loginfail_detect

import java.lang

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 风控类需求：恶意登录   2s内连续登录失败
object LoginFail {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEvent = env.readTextFile(resource.getPath)
      .map(line=>{
        val arr: Array[String] = line.split(",")
        UserLoginEvent(arr(0).toLong,arr(1),arr(2),arr(3)toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp*1000
      })

    // 使用processfunction实现连续登录失败的检测
    val logfarilWarngingStream = loginEvent
        .keyBy(_.userId)
        .process( new LogfailProcessFunction(3))

    logfarilWarngingStream.print()



    env.execute("login fail job")
  }
}

// 定义输入输出样例类
case class UserLoginEvent(userId:Long,ip:String,eventTpye:String,timestamp:Long)
case class LoginFailWarning(userId:Long,firstfailtime:Long,lastfailtime:Long,msg:String)

class LogfailProcessFunction(failtimes :Int) extends KeyedProcessFunction[Long,UserLoginEvent,LoginFailWarning]{

  // 定义状态，保存2s内所有登录事件的列表，  定时器时间戳(需要一个即可)
  lazy val logfailEventListState:ListState[UserLoginEvent]= getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("logfailEventListState",classOf[UserLoginEvent]))
  lazy val timeTsState:ValueState[Long]= getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeTsState",classOf[Long]))

  override def processElement(value: UserLoginEvent,
                              ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context,
                              out: Collector[LoginFailWarning]): Unit = {

    // 判读当前数据是否登录成功还是失败
    if(value.eventTpye == "fail"){
      // 1.如果失败，添加数据列表状态中
      logfailEventListState.add(value)
      //如果没有定时器，注册一个
      if(timeTsState.value() == 0){
        val ts = value.timestamp*1000 + 2000
        ctx.timerService().registerEventTimeTimer(ts)
        timeTsState.update(ts)
      }
      }else {
      // 2. 如果是成功，直接删除定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timeTsState.value())
      logfailEventListState.clear()

    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit ={

    import scala.collection.JavaConversions._
    val loginfailList: lang.Iterable[UserLoginEvent] = logfailEventListState.get()
    // 定时器触发，说明没有登录成功，要判断有多少次登录失败
    if(loginfailList.size >= failtimes){

      // 超过定义的失败上限，输出报警信息
      out.collect(LoginFailWarning(ctx.getCurrentKey,
        loginfailList.head.timestamp,
        loginfailList.last.timestamp,
        s"login fail in 2s for ${loginfailList.size } times"))
    }

    // 清空状态
    logfailEventListState.clear()
    timeTsState.clear()

  }
}
