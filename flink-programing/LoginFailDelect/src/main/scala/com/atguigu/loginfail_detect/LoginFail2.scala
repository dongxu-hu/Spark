package com.atguigu.loginfail_detect

import java.lang

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 风控类需求：恶意登录   2s内连续登录失败,
// 升级版 对次数管控，次数超过2s就开始报警，不是等到2s时间后才报警
// 只能监控连续数据
//  在2s内有成功，就删除了报警记录
object LoginFail2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEvent = env.readTextFile(resource.getPath)
      .map(line=>{
        val arr: Array[String] = line.split(",")
        UserLoginEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp*1000
      })

    // 使用processfunction实现连续登录失败的检测
    val logfarilWarngingStream = loginEvent
        .keyBy(_.userId)
        .process( new LogfailProcessFunction2())

    logfarilWarngingStream.print()



    env.execute("login fail pro job")
  }
}


class LogfailProcessFunction2 extends KeyedProcessFunction[Long,UserLoginEvent,LoginFailWarning]{

  // 定义状态，保存2s内所有登录事件的列表
  lazy val logfailEventListState:ListState[UserLoginEvent]= getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("logfailEventListState",classOf[UserLoginEvent]))

  override def processElement(value: UserLoginEvent,
                              ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context,
                              out: Collector[LoginFailWarning]): Unit = {

    // 判读当前数据是否登录成功还是失败
    if(value.eventTpye == "fail"){
      // 1.如果失败，判断之前是否一有登录失败事件
      val iter = logfailEventListState.get().iterator()
      if(iter.hasNext ){
        // 1.1 如果已有登录失败，继续判断是否在2s之内
        val firstFaiEvent = iter.next()
        if(value.timestamp - firstFaiEvent.timestamp <= 2){
          out.collect(LoginFailWarning(value.userId,firstFaiEvent.timestamp,value.timestamp,
            s"login fail in 2s for 2 times"))
        }
        // 不管是否报警，直接清空状态，将最近一次失败数据添加进去
        logfailEventListState.clear()
        logfailEventListState.add(value)
      }else{
        // 1.2 没有的数据的话，当前第一次登录失败，直接添加
        logfailEventListState.add(value)
      }
      }else {
      // 2. 如果是成功，重新开始
      logfailEventListState.clear()
    }
  }
  }
