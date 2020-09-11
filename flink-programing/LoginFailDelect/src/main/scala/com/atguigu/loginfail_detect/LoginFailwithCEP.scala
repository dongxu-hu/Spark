package com.atguigu.loginfail_detect

import java.{lang, util}


import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 风控类需求：恶意登录   2s内连续登录失败,
// 升级版 对次数管控，次数超过2s就开始报警，不是等到2s时间后才报警
// 只能监控连续数据
//  在2s内有成功，就删除了报警记录
object LoginFailwithCEp {
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

    // 2. 定义一个模式匹配，用来检测复杂事件序列
    //2.1 使用 next 严格近邻
    val loginFailPattern = Pattern.begin[UserLoginEvent]("firstFail")
        .where(_.eventTpye == "fail")
        .next("secondFail").where(_.eventTpye == "fail")
        .next("Fail").where(_.eventTpye == "fail")
        .within(Time.seconds(5))

    //2.2 使用  times + consecutive 严格近邻,多次数； 如果不添加consecutive，默认为宽松近邻
/*    val loginFailPattern = Pattern.begin[UserLoginEvent]("fail")
      .where(_.eventTpye == "fail")
      .times(3).consecutive()
      .within(Time.seconds(5))*/




    // 3. 对数据流应用定义好的模式，得到patternStream
    val patternStream = CEP.pattern(loginEvent.keyBy(_.userId),loginFailPattern)

    //4. 检出复合匹配条件的事件序列
    val loginFailDataStream = patternStream.select( new LoginFailSelect())


    // 5. 打印输出
    loginFailDataStream.print()

    env.execute("login fail pro job")
  }
}

// 实现自定义的patternSelect
class LoginFailSelect() extends PatternSelectFunction[UserLoginEvent,LoginFailWarning]{
  override def select(pattern: util.Map[String, util.List[UserLoginEvent]]): LoginFailWarning = {

    // 从map结构中可以拿到第一次第二次登陆失败的事件

    // 对应 2.1 方案
    val firstFailEven = pattern.get("firstFail").get(0)
    val SecondFailEven = pattern.get("Fail").get(0)
    LoginFailWarning(firstFailEven.userId,firstFailEven.timestamp,SecondFailEven.timestamp,"login fail")

    // 对应 2.2 方案
/*    val firstFailEven = pattern.get("fail").get(0)
    val SecondFailEven = pattern.get("fail").get(2)
    LoginFailWarning(firstFailEven.userId,firstFailEven.timestamp,SecondFailEven.timestamp,"login fail")*/
  }
}
