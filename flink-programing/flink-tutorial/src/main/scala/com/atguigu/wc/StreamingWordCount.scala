package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

//  流处理
object StreamingWordCount {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 接收socket文本流
   // val paramToool: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = args(0)
    val port: Int = args(1).toInt
    //val host: String = paramToool.get("host")
   // val port: Int = paramToool.getInt("port")
    val inputDataStream: DataStream[String] = environment.socketTextStream(host,port )

    //3. 对dataStream进行转换处理
    val resultDataStram: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //4. 打印输出
    resultDataStram.print()

    //5. 指定执行任务
    environment.execute()

  }

}
