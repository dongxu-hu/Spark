package com.atguigu.api.sink

import java.util.Properties

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}



// 写出到kafka
object KafkaSink {
  def main(args: Array[String]): Unit = {
    // 创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("group.id", "consumer-group")
    val dataStrem: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("text", new SimpleStringSchema(), properties))


//    val dateDateStream: DataStream[SensorReading] = dataStrem.map(line => {
//      val arr: Array[String] = line.split(",")
//      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
//    })


    // 写入kafka
    dataStrem
      .addSink( new FlinkKafkaProducer011[String]("hadoop202:9092", "sensor", new SimpleStringSchema()) )


    environment.execute()
  }

}
