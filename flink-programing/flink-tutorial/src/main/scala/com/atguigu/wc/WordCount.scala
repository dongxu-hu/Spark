package com.atguigu.wc

import org.apache.flink.api.scala._


// 批处理
object WordCount {
  def main(args: Array[String]): Unit = {

    //创建环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    val inputDS: DataSet[String] = environment.readTextFile("F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\hello.txt")

    //数据处理
    val resultDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    //结果输出
    resultDS.print()


  }
}
