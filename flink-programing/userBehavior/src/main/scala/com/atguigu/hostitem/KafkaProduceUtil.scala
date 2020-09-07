package com.atguigu.hostitem

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProduceUtil {
  def main(args: Array[String]): Unit = {
      writeTokafka("hostitem")
  }

  def writeTokafka(topic:String): Unit ={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    val producer = new KafkaProducer[String, String](properties)

    //从文件读取数据
    val bufferedSource = io.Source.fromFile("F:\\MyWork\\GitDatabase\\flink-programing\\userBehavior\\src\\main\\resources\\UserBehavior.csv")

    for (elem <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String,String](topic,elem)
      producer.send(record)

    }
    producer.close()

  }
}
