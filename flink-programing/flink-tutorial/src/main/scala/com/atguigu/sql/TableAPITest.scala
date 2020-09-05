package com.atguigu.sql


import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row


// 流处理环境搭建
object TableAPITest {
  def main(args: Array[String]): Unit = {

    // 1. 执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment)


    // 2. 创建表
    // 从文件中读取数据
    val filePath = "F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
      .field("id",DataTypes.STRING())
      .field("timestamp",DataTypes.BIGINT())
      .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val sensorTable: Table = tableEnv.from("inputTable")


    // 3. 表的查询
    val aggTable: Table = sensorTable.select('id, 'temperature)
      .filter("id ='sensor_1'")

    // 4.输出
    // 输出到kafka
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("sinkTest")
        .property("zookeeper.connect","hadoop202:2181")
        .property("bootstrap.servers","hadoop202:9092"))
        .withFormat(new Csv())
        .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE()))
        .createTemporaryTable("kafkaOutpuTable")

    aggTable.insertInto("kafkaOutpuTable")



    environment.execute("table api test")
  }

}
