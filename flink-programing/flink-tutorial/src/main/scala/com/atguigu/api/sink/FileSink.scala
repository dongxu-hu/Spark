package com.atguigu.api.sink

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._


// sink写入文件中
object FileSink {
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


    val Output ="F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Output.Txt"
    //方法一   过时
//    dateDateStream.writeAsCsv(Output)

    // 方法二
    dateDateStream.addSink(StreamingFileSink.
      forRowFormat(new Path(Output),new SimpleStringEncoder[SensorReading]("UTF-8")).build())



    environment.execute()

  }


}
