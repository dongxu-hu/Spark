package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//  热门商品top10
// 每个品类 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数

object TopN01 {
  def main(args: Array[String]): Unit = {

    // 创建环境
    val sparkContext = new SparkContext(new SparkConf().setAppName("TopN01").setMaster("local[*]"))

    // 读取数据
    val inpuRDD: RDD[String] = sparkContext.textFile("F:\\MyWork\\GitDatabase\\spark\\spark-core\\src\\main\\input\\user_visit_action.txt")

    // 读入数据结构转换
    val dataRDD: RDD[UserVisitAction] = inpuRDD.map(line => {
      val arr: Array[String] = line.split("_")
      UserVisitAction(arr(0),
        arr(1).toLong,
        arr(2),
        arr(3).toLong,
        arr(4),
        arr(5),
        arr(6).toLong,
        arr(7).toLong,
        arr(8),
        arr(9),
        arr(10),
        arr(11),
        arr(12).toLong)
    })

    // 取出想要数据进行下一步处理
    // 转换结果为  (品类A,1,0,0)
    val infoRDD: RDD[CategoryCountInfo] = dataRDD.flatMap(useraction => {
      // 判断是否为点击行为
      if (useraction.click_category_id != -1) {
        val clickList = new ListBuffer[CategoryCountInfo]
        clickList.append(CategoryCountInfo(useraction.click_category_id.toString, 1L, 0L, 0L))
        clickList
      } else if (useraction.order_category_ids != "null") {
        // 判断是否为订单行为
        val orderList = new ListBuffer[CategoryCountInfo]
        val arr: Array[String] = useraction.order_category_ids.split(",")
        for (elem <- arr) {
          orderList.append(CategoryCountInfo(elem, 0L, 1L, 0L))
        }
        orderList
      } else if (useraction.pay_category_ids != "null") {
        // 判断是否为 支付行为
        val payList = new ListBuffer[CategoryCountInfo]
        val arr: Array[String] = useraction.pay_category_ids.split(",")
        for (elem <- arr) {
          payList.append(CategoryCountInfo(elem, 0L, 0L, 1L))
        }
        payList
      } else {
        // Nil  是空集合
        Nil
      }
    })

    // 依据类别分组，
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(_.categoryId)

    // 举行聚合求和
    val reduceRDD: RDD[(String, CategoryCountInfo)] = groupRDD.mapValues(datas => {
      datas.reduce((of1, of2) => {
         val of1.clickCount  = of1.clickCount  +of2.clickCount
         val of1.orderCount = of1.orderCount +of2.orderCount
         val of1.payCount = of1.payCount +of2.payCount
        of1
      })
    })


    // 结构转换，结果输出
    val mapRDD: RDD[CategoryCountInfo] = reduceRDD.map(_._2)

    val res: Array[CategoryCountInfo] = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false).take(10)
//    resultRDD.take(10).foreach(println)

    //--------------需求二--------------------
    //	通过需求1，获取TopN热门品类的id
    val categoryIDs: Array[String] = res.map(_.categoryId)

    //	将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRDD: RDD[UserVisitAction] = dataRDD.filter(
      userAction => {
        //坑1：   热门品类id是字符串数组，是否包含 获取的userAction.click_category_id是long类型，需要转换为字符串
        userAction.click_category_id != -1 && categoryIDs.contains(userAction.click_category_id.toString)
      }
    )

    //	对session的点击数进行转换 (category-session,1)
    //坑2：session-id中的值有-，所以我们这里的连接符号避免-
    val mapRDD11: RDD[(String, Int)] = filterRDD.map(
      userAction => (userAction.click_category_id + "_" + userAction.session_id, 1)
    )

    //	对session的点击数进行统计 (category-session,sum)
    val reduceRDD11: RDD[(String, Int)] = mapRDD11.reduceByKey(_+_)

    //	将统计聚合的结果进行转换  (category,(session,sum))
    val mapRDD22: RDD[(String, (String, Int))] = reduceRDD11.map {
      case (cateAndSession, count) => {
        (cateAndSession.split("_")(0), (cateAndSession.split("_")(1), count))
      }
    }

    //	将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val groupRDD22: RDD[(String, Iterable[(String, Int)])] = mapRDD22.groupByKey()

    //	对分组后的数据降序 取前10
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD22.mapValues(
      datas => datas.toList.sortBy(-_._2).take(10)
    )
    resRDD.collect().foreach(println)

    sparkContext.stop()
  }
}

//用户访问动作表
case class UserVisitAction(date: String,  //用户点击行为的日期
                           user_id: Long, //用户的ID
                           session_id: String,  //Session的ID
                           page_id: Long, //某个页面的ID
                           action_time: String, //动作的时间点
                           search_keyword: String,  //用户搜索的关键词
                           click_category_id: Long, //某一个商品品类的ID
                           click_product_id: Long,  //某一个商品的ID
                           order_category_ids: String,  //一次订单中所有品类的ID集合
                           order_product_ids: String,   //一次订单中所有商品的ID集合
                           pay_category_ids: String,    //一次支付中所有品类的ID集合
                           pay_product_ids: String,   //一次支付中所有商品的ID集合
                           city_id: Long)   //城市 id



// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             clickCount: Long,//点击次数
                             orderCount: Long,//订单次数
                             payCount: Long)//支付次数