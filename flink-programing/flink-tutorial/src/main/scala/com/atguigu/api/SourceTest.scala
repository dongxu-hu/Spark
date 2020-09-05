package com.atguigu.api

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import scala.collection.immutable
import scala.util.Random


case class SensorReading(id: String, timestamp: Long, temperature: Double)
object SourceTest {
  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    // 1. 从集合获取数据
    val stream1 = environment
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1),
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1)
      ))

//     stream1.print().setParallelism(1)


    //2. 从文本读取数据
    val file ="F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.Txt"
    val inputStram: DataStream[String] = environment.readTextFile(file)
//    inputStram.print("stram2").setParallelism(1)

    //3. 自定义数据读取
    val value: DataStream[Any] = environment.fromElements(0, 6, 0.67, "hello")
    //value.print()

    //4. 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("group.id", "consumer-group")

    val inputStraeam4: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    inputStraeam4.print()


    //5. 自定义source
    val inputStream5: DataStream[SensorReading] = environment.addSource(new MySensorSource)
//    val inputStream5: DataStream[SensorReading] = environment.addSource(new mySouce)
    inputStream5.print()

    //执行
    environment.execute("stram5")


  }
}

// 自定义的SourceFunction, 继承SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{

  // 定义一个标识位，用来指示是否正常生成数据
  var running : Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val random = new Random()
    // 随机初始化10个传感器的温度器，之后在此基础上随机波动
    val curTemList: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor_" + i, 60 + (random.nextGaussian())*10 ))
    // 无限循环，生成随机的传感器数据
    while (running){
      // 在之前温度基础上随机波动一点，改变温度值
      val curTempList: immutable.IndexedSeq[(String, Double)] = curTemList.map(
        curTem => (curTem._1, curTem._2+random.nextGaussian() )
        //curTem => (curTem._1, curTem._2 + random.nextGaussian())
      )

      // 获取当前的时间戳
      val curTs: Long = System.currentTimeMillis()
      // 将数据包装成样例类，用ctx输出
      curTempList.foreach(
        curTemTuple =>sourceContext.collect(SensorReading(curTemTuple._1,curTs,curTemTuple._2))
      )
      // 间隔1s
      Thread.sleep(5000)
    }
  }

  override def cancel(): Unit = false
}

class mySouce extends SourceFunction[SensorReading]{

  val bool :Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    val random = new Random()
    val tuples: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("first_" + i, 50 + random.nextGaussian()))

    while (bool){
      val time: Long = System.currentTimeMillis()
      tuples.foreach(tem=>sourceContext.collect(SensorReading(tem._1,time,tem._2)))
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = false
}