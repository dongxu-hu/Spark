package com.atguigu.sql

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeandWindowTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    // 设定时间字段的方式
    //2. 读取数据
    val filePath = "F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.txt"
    val inputStram: DataStream[String] = environment.readTextFile(filePath)
    val dateDateStream: DataStream[SensorReading] = inputStram.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(500)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000L
        })

//    //  定义处理时间
//    // 将流转换为Table,同时设定时间字段
//    val sensorTable: Table = tableEnv.fromDataStream(dateDateStream, 'id, 'temperature as 'temp, 'timestamp as 'ts, 'pt.proctime)

      // 定义事件时间
    val sensorTable: Table = tableEnv.fromDataStream(dateDateStream, 'id, 'temperature as 'temp, 'timestamp.rowtime as 'ts)

    //3. 窗口聚合
    // 3.1 分组窗口 table API实现
    val resulttable: Table = sensorTable.window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count as 'cnt, 'tw.end)

    // 转换成流打印输出   toAppendStream 窗口结束时仅输出一次，可以正常使用
//    resulttable.toAppendStream[Row].print()

//      sensorTable.printSchema()
//      sensorTable.toAppendStream[Row].print()


    //3.1.2 分组窗口  SQL 实现
    tableEnv.createTemporaryView("sensor",sensorTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, count(id) as cnt, tumble_end(ts,interval '10' second)
        |from sensor
        |group by id, tumble(ts,interval '10' second)
        |""".stripMargin
    )

//    resultSqlTable.printSchema()
//    resultSqlTable.toAppendStream[Row].print("sql")

    // 3.2 over Window
    // 3.2.1 table API实现
    val overResultTable = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
        .select('id,'ts,'id.count over 'ow, 'temp.avg over 'ow)

    overResultTable.printSchema()
    overResultTable.toAppendStream[Row].print("over")


    // 3.2.2  SQL
    val overResultSQLTable = tableEnv.sqlQuery(
      """
        |select id, ts, count(id) over ow, avg(temp) over ow
        |from sensor
        |window ow as (
        |partition by id
        |order by ts
        |rows between 2 preceding and current row)
        |""".stripMargin
    )

     overResultSQLTable.printSchema()
      overResultSQLTable.toAppendStream[Row].print("sqlover")






    environment.execute("time and window test job")
  }

}
