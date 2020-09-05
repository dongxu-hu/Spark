package com.atguigu.api.sink

import java.util

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.http.HttpHost
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

// 写入ES
object ESSink{
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

    // 写入es
    // 定义HttpHost
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop202", 9200))

    dateDateStream.addSink( new ElasticsearchSink
    .Builder[SensorReading](httpHosts, new MyESSink)
      .build()
    )

    environment.execute()


  }

}

class MyESSink extends ElasticsearchSinkFunction[SensorReading]{
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

    //提取数据包装sorce
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id",t.id)
    dataSource.put("timestamp",t.timestamp.toString)
    dataSource.put("temperature",t.temperature.toString)

    //创建index request
    val request: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("tem")
      .source(dataSource)

    //利用requestIndexer发送http请求
    requestIndexer.add(request)

    println(t+"saved")
  }
}

class myEs extends ElasticsearchSinkFunction[SensorReading]{
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

    // 将数据包装sorce
    val hashMap = new util.HashMap[String, String]()
    hashMap.put("id",t.id)
    hashMap.put("timestamp",t.timestamp.toString)
    hashMap.put("temperature",t.temperature.toString)

    //创建index request
    val request: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("doc_")
      .source(hashMap)
    requestIndexer.add(request)


  }
}
