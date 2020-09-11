package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderInfo, StartUpLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val scc = new StreamingContext(sparkConf, Seconds(5))


    //2.读取Kafka 主题的数据创建流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, scc)


    //4.将读取的数据转换为样例类对象(logDate和logHour)
    val orderInfoDstrearm : DStream[OrderInfo] = kafkaStream.map(record => {

      //a.取出Value
      val value: String = record.value()

      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      val createtimearr: Array[String] = orderInfo.create_time.split(" ")


      orderInfo.create_date = createtimearr(0)
      orderInfo.create_hour = createtimearr(1).split(":")(0)

      val str: String = orderInfo.consignee_tel.substring(0, 3)

      orderInfo.consignee_tel = str + "*******"
      orderInfo
    })

    //5. 写出
    orderInfoDstrearm.foreachRDD( rdd=>{
      rdd.saveToPhoenix("GMALL200317_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toString.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop202,hadoop203,hadoop204:2181"))
    }
     )

    scc.start()
    scc.awaitTermination()
  }
}
