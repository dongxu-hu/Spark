package com.atguigu.sql

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

// 未运行，仅仅是总结
object Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // 0. 创建Flink环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.0 创建表的执行环境  可通过EnvironmentSettings配置溢写执行环境的参数配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    // 2.1.1  使用本地文件注册表
    tableEnv.connect(new FileSystem().path("F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\Souce.Txt"))
      .withFormat(new OldCsv())  // 定义从外部读取文件后进行格式化处理，使用OldCsv
      .withSchema(new Schema()   // 定义表中的字段
        .field("id",DataTypes.STRING())  // 定义字段名，字段类型
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")   // 创建SQL中表，可直接直接进行查询，查询返回结果Table

    // 2.1.2 依据实际sql中的表创建可操作Table类型对象，进行Table API操作
    val LocalTable: Table = tableEnv.from("inputTable")


    // 2.2.1 从kafka读取数据注册表及视图
    tableEnv.connect(
      new Kafka()
        .version("0.11")   // 定义kafka的版本
        .topic("sensor")   // 定义主题
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")


    // 2.3 将DataStream转换为表
    // 2.3.1 方法一  可以对字段名起别名（可以基于字段名字或位置（不带字段名）），通过rowtime增加事件时间字段，需提前开启事件时间模式
    //2. 读取数据
    val inputStram: DataStream[String] = environment.socketTextStream("hadoop202", 7777)

    val dateDateStream: DataStream[SensorReading] = inputStram.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sensorTable1: Table = tableEnv.fromDataStream(dateDateStream, 'id, 'temperature as 'temp, 'timestamp.rowtime as 'ts)

    // 2.3.2 方法二 可以对字段名起别名，通过proctime增加处理时间字段
    val sensorTable2: Table = tableEnv.fromDataStream(dateDateStream, 'id, 'temperature as 'temp, 'timestamp as 'ts, 'pt.proctime)

    // 3.1 Table API 查询
    // 3.1.1使用 Table类型对象操作,返回结果是Table类型，可再次进行查询操作
    val aggTable: Table = sensorTable1.groupBy('id)
      .select('id, 'id.count as 'count)


    // 3.2 SQL 查询
    // 3.2.1 使用 建表环境调用sqlQuery方法操作， 返回值是Table，若再次进行SQL操作，需转化为视图
    // SQL 写法一：
    val resultSqlTable: Table = tableEnv.sqlQuery("select id from inputTable")
    // SQL 写法二：
    val resultSqlTable1: Table = tableEnv.sqlQuery(
      """
        |select id, temperature as temp
        |from inputTable
        |where id = 'sensor_1'
    """.stripMargin)

    // 3.2.2 将第一次查询结果转换为视图，再次进行SQL操作
    // 3.2.2.1 创建视图方式一：基于Table
    //新的SQL表名为newtable
    tableEnv.createTemporaryView("newtable",resultSqlTable)
    // 3.2.2.2 创建视图方式二：基于DataStream
    tableEnv.createTemporaryView("newtable", dateDateStream)
    // 3.2.2.3 创建视图方式三：基于DataStream中字段
    tableEnv.createTemporaryView("newtable", dateDateStream, 'id, 'temperature, 'timestamp as 'ts)

    // 3.2.2.4 第二次SQL查询
    val table2: Table = tableEnv.sqlQuery(
      """
        |select id
        |from newtable
        |where id = 'sensor_1'
        |""".stripMargin)


    // 4.1 控制台输出
    table2.printSchema() // 打印字段信息；（字段名，字段类型）
    table2.toAppendStream[Row].print("1")  //以追加的形式打印在控制台


    // 4.2 输出到本地文件
    // 4.2.1 注册输出表
    tableEnv.connect(new FileSystem().path("本地路径"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE()))
        .createTemporaryTable("outputTable")

        //4.2.2 数据写入到输出表，采用inserInto方式
        resultSqlTable.insertInto("outputTable")


    //4.3 输出到kafka
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("sinkTest")
        .property("zookeeper.connect", "Hadoop202:2181")
        .property("bootstrap.servers", "Hadoop202:9092"))
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultSqlTable.insertInto("kafkaOutputTable")


    // 4.4 输出到ES
    // 4.4.1 导入依赖
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-json</artifactId>
      <version>1.10.1</version>
    </dependency>

    // 4.2 实现代码
    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("Hadoop02", 9200, "http")
        .index("sensor")
        .documentType("temp"))
      .inUpsertMode()           // 指定是 Upsert 模式
      .withFormat(new Json())
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")

    resultSqlTable.insertInto("esOutputTable")


    // 4.5 输出到MySQL
    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |  id varchar(20) not null,
        |  cnt bigint not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://localhost:3306/test',
        |  'connector.table' = 'sensor_count',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = '123456'
        |)
  """.stripMargin

    tableEnv.sqlUpdate(sinkDDL)
    resultSqlTable.insertInto("jdbcOutputTable")





    environment.execute("table API and SQL test")
  }
}
