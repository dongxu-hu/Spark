package com.atguigu.state

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//
object StateTest2 {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.socketTextStream("hadoop202", 7777)
    val dateStream: DataStream[SensorReading] = inputStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })

   // val keyedData: KeyedStream[SensorReading, String] = dateStream.keyBy(0).map(tem=>(tem,"9/2"))

    val alerts: DataStream[(String, Double, Double)] = dateStream
      .flatMap(new TemperatureAlertFunction(1.7))


  }
}


class TemperatureAlertFunction(val threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  override def flatMap(reading: SensorReading,
                       out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    val tempDiff = (reading.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      out.collect((reading.id, reading.temperature, tempDiff))
    }
    this.lastTempState.update(reading.temperature)
  }
}