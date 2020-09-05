package com.atguigu.sql

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._


object Example {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 读取数据
    val inputStram: DataStream[String] = environment.socketTextStream("hadoop202", 7777)

    val dateDateStream: DataStream[SensorReading] = inputStram.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //基于环境创建一个表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    // 基于一条流创建一张表, 流中的样例类中的字段对应表的列
    val datatable: Table = tableEnv.fromDataStream(dateDateStream)


    // 1. 调用table API  进行表的转换操作
//    val resulttable: Table = datatable.select("id,temperature").filter("id =='sensor_1'")
//    resulttable.printSchema()
//    val resultDataStream: DataStream[(String, Double)] = resulttable.toAppendStream[(String, Double)]
//    resultDataStream.print("res")

    // 2. 直接写sql， 得到转换结果
    tableEnv.createTemporaryView("datatable",datatable)
    val sql = "select id,temperature from datatable where id ='sensor_1'"
    val resultSql: Table = tableEnv.sqlQuery(sql)
        val resultDataStream: DataStream[(String, Double)] = resultSql.toAppendStream[(String, Double)]
        resultDataStream.print("ressql")



    environment.execute("table api example")


  }
}
