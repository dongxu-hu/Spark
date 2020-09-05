package com.atguigu.api


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object TransFromTest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 从文本读取数据
    val file ="F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.Txt"
    val inputStram: DataStream[String] = environment.readTextFile(file)

    val dateDateStream: DataStream[SensorReading] = inputStram.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //dateDateStream.print()


    // 使用keyBy + minBy聚合
    val minBy: DataStream[SensorReading] = dateDateStream.keyBy("id").minBy(2)
//    minBy.print()

    // 使用keyBy + minBy聚合
    val min: DataStream[SensorReading] = dateDateStream.keyBy("id").min(2)
//    min.print()


    // 一般化聚合：输出（id， 最新时间戳，最小温度值）
    val agge: DataStream[SensorReading] = dateDateStream.keyBy("id")
      .reduce((cur, newDate) => SensorReading(newDate.id, newDate.timestamp + 1,
        cur.temperature.min(newDate.temperature)))
//    agge.print("stram2")



    // 3.1 分流	split 和  select
    val splStream: SplitStream[SensorReading] = dateDateStream.split(tem => {
      if (tem.temperature > 30) List("high") else List("low")
    })
    val high: DataStream[SensorReading] = splStream.select("high")
    val low: DataStream[SensorReading] = splStream.select("low")
    val all: DataStream[SensorReading] = splStream.select("high","low")
//    high.print()

//    low.print()
//    all.print()


    //3.2  连接两条流
    val temDataStream: DataStream[(Double, String)] = low.map(tem => (tem.temperature, tem.id))
    val connStream: ConnectedStreams[(Double, String), SensorReading] = temDataStream.connect(high)
    val resultStram: DataStream[(Any, Any)] = connStream.map(tem => (tem._1, tem._2), tem1 => (tem1.id, tem1.timestamp))

//    resultStram.print()

    //3.3 连接两条流union
    val higlowStram: DataStream[SensorReading] = high.union(low).union(all)
//    higlowStram.print()


    // 使用函数类
    val value: DataStream[(String, Double)] = dateDateStream.map(new MyMapper)
    value.print()

    environment.execute()

  }
}

// 自定义函数类
class MyMapper extends MapFunction[SensorReading, (String, Double)]{
  override def map(value: SensorReading): (String, Double) = (value.id,value.temperature)
}
