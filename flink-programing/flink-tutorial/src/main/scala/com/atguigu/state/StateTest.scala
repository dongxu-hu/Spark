package com.atguigu.state

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object StateTest {

  // 温度跳变实现
  def main(args: Array[String]): Unit = {
    //  环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend(""))
    env.setStateBackend(new RocksDBStateBackend(""))

    val inputStream: DataStream[String] = env.socketTextStream("hadoop202",7777)

    val dateStream: DataStream[SensorReading] = inputStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })

    val warnstream: DataStream[(String, Double, Double)] = dateStream
      .keyBy(_.id)
//      .flatMap(new TemchangeWarning(10.0))
        .flatMapWithState[(String,Double,Double),Double](
          {
            case (inputData: SensorReading,None)=>(List.empty,Some(inputData.temperature))
            case (inputData: SensorReading,lastTemp:Some[Double])=> {
              val diff = (inputData.temperature-lastTemp.get).abs
              if(diff > 10.0){
                (List((inputData.id,lastTemp.get,inputData.temperature)),Some(inputData.temperature))
              }else{
                (List.empty,Some(inputData.temperature))
              }
            }

          }
        )

    warnstream.print()

    env.execute()


    }

}

// 自定义RichFlatMapFunction，实现温度跳变报警功能
class TemchangeWarning(threshod: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{

  // 定义状态，保存上一个温度值
  var lastTemState: ValueState[Double]= _
  var defultTem= -273.15

  // 加入一个标识位状态，用来表示是否串行过传感器数据
  var isoccurstate: ValueState[Boolean]= _

  override def open(parameters: Configuration): Unit =
    {lastTemState =getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp",classOf[Double],defultTem))
      isoccurstate =getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("last_temp",classOf[Boolean]))}

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

      // 从状态中获取上次温度值
      val lastTem: Double = lastTemState.value()
      val isbool: Boolean = isoccurstate.value()

    //取当前温度做比较，如果大于阈值，输出报警信息
      val diff =(value.temperature-lastTem).abs
//    if(diff > threshod  &&  lastTem != defultTem){
    if(diff > threshod  &&  isbool){
      out.collect(value.id,lastTem,value.temperature)
    }

    // 变更状态
    lastTemState.update(value.temperature)
    isoccurstate.update(true)
  }

}
