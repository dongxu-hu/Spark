package com.atguigu.api.sink

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}



object RedisSink {
  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 从文本读取数据
    val file ="F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.Txt"
    val inputStram: DataStream[String] = environment.readTextFile(file)

    val dateDateStream: DataStream[SensorReading] = inputStram.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop202")
      .setPort(6379)
      .build()

    dateDateStream.addSink(new RedisSink[SensorReading](config,new MyRdisMapper()))

    environment.execute()

  }

}

class MyRdisMapper() extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor")
//  new RedisCommandDescription(RedisCommand.SET,"fist")


  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}


