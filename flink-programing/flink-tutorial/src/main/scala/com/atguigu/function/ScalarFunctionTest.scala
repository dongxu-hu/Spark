package com.atguigu.function

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.types.Row

object ScalarFunctionTest {

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
        .field("timestamp",DataTypes.BIGINT())  // timestamp是关键字，使用时需注意
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val sensorTable: Table = tableEnv.from("inputTable")

    //1. Table API 中使用
    // 创建UDF实例，然后直接使用
    val hashCode1 = new HashCode(5)
    val resultTable1: Table = sensorTable
      .select('id, 'timestamp, 'temperature, hashCode1('id))

//    resultTable1.toAppendStream[Row].print("res")


    // SQL 中使用
    // 在环境中注册UDF
//    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("HashCode1",hashCode1)
    val resultSQlTable: Table = tableEnv.sqlQuery("select id,`timestamp`,temperature,HashCode1(id) from inputTable")
    resultSQlTable.toAppendStream[Row].print("sql")



   // 1. Table API中使用
   val avgTemp = new AvgTemp
    val resultTable2: Table = sensorTable
      .groupBy('id)
      .aggregate(avgTemp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)
//    resultTable2.toRetractStream[Row].print("res")

    // SQL 中使用
    // 在环境中注册UDF
    tableEnv.registerFunction("avgTemp",avgTemp)
//     val resultSQlTable2: Table = tableEnv.sqlQuery("select id,avgTemp(temperature) from sensor group by id")
//      resultSQlTable2.toRetractStream[Row].print("sql")



    environment.execute("test")

  }

}


// 自定义标量函数

class HashCode(factor: Int) extends ScalarFunction{

  def eval (value: String):Int={
      value.hashCode*factor

  }

}



// 专门定义一个聚合状态类型
class AvgTemAcc{
  var Count: Int = 0
  var Sum : Double = 0.0
}

// 自定义聚合函数
class  AvgTemp extends  AggregateFunction[Double,AvgTemAcc]{
  override def getValue(accumulator: AvgTemAcc): Double = accumulator.Sum /accumulator.Count

  override def createAccumulator(): AvgTemAcc = new AvgTemAcc

  def accumulate(acc:AvgTemAcc,temp :Double):Unit={
    acc.Sum += temp
    acc.Count += 1

  }
}