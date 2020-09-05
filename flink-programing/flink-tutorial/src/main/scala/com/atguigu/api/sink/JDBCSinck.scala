package com.atguigu.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.api.{MySensorSource, SensorReading}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._


// 写入MySQL
object JDBCSinck {
  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 从文本读取数据
    val file ="F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.Txt"
    val inputStram: DataStream[String] = environment.readTextFile(file)

    val inputStream4 = environment.addSource( new MySensorSource() )


    // 基本转换
    val dataStream: DataStream[SensorReading] = inputStream4
/*    val dateDateStream: DataStream[SensorReading] = inputStram.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })*/


    // 写入mySQL
    dataStream.addSink( new MyJdbcSink() )

    environment.execute("jdbc sink job")


  }

}



// 自定义实现SinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{

  // 定义sql连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 创建连接，并实现预编译语句
    conn = DriverManager.getConnection("jdbc:mysql://hadoop202:3306/user", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temperature) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: _root_.org.apache.flink.streaming.api.functions.sink.SinkFunction.Context[_]): Unit = {
    // 直接执行更新语句，如果没有更新就插入
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}