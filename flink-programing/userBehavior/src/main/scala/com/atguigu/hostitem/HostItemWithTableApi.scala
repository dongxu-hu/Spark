package com.atguigu.hostitem

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HostItemWithTableApi {
  def main(args: Array[String]): Unit = {

    // 创建环境及配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换为样例类，提起时间戳设置watermark
    val inputStream: DataStream[String] = env.readTextFile("F:\\MyWork\\GitDatabase\\flink-programing\\userBehavior\\src\\main\\resources\\UserBehavior.csv")


    // 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop202:9092")

//    val inputStream: DataStream[String] =env.addSource( new FlinkKafkaConsumer[String]("hostitem",new SimpleStringSchema(),properties))


    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000)   // 获取watermark，时间戳提取器


    val settings =  EnvironmentSettings.newInstance()
      .useAnyPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 将DataStream转换为表
    val dataTable = tableEnv.fromDataStream(dataStream,'itemId,'behavior,'timestamp.rowtime as 'ts)


    // 基于tableAPI 进行窗口聚合
    val aggTable = dataTable.filter('behavior ==="pv")
        .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
        .groupBy('itemId,'sw)
        .select('itemId,'itemId.count as 'cnt,'sw.end as 'windowEnd)

    aggTable.toAppendStream[Row].print("agg")

    // SQl 排序输出
    tableEnv.createTemporaryView("agg",aggTable,'itemId,'cnt,'windowEnd)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from(
        |   select *,
        |   row_number() over
        |   (partition by windowEnd order by cnt desc)
        |   as row_num
        |   from agg
        |)
        | where row_num <=5
        |""".stripMargin)


    resultTable.toRetractStream[Row].print("res")

    env.execute("test")

  }

}
