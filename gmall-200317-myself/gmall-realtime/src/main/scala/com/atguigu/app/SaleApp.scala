package com.atguigu.app


import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDate
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

object SaleApp {
  def main(args: Array[String]): Unit = {

    // 1. order_info 和 order_detail 双流full outor join

          //1.1 获取sparkcontext
          val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
          val sparkContext = new StreamingContext(sparkConf, Seconds(5))

          // 1.2 从kafka中获取 order_info 和order_detail 数据
          val inputOrderDstream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, sparkContext)
          val inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, sparkContext)

          // 1.3 双流full outor join
                       // 双流join 前 要把流变为kv结构
                      // 订单表中电话号码脱敏，日期格式修改；字符串转换为样例类对象
                      val orderInfoDstream: DStream[OrderInfo] = inputOrderDstream.map { record =>
                            val jsonString: String = record.value()
                            // 1 转换成case class
                            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                            // 2  脱敏 电话号码  1381*******
                            val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
                            orderInfo.consignee_tel = telTuple._1 + "*******"
                            // 3  补充日期字段
                            val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
                            orderInfo.create_date = datetimeArr(0) //日期
                            val timeArr: Array[String] = datetimeArr(1).split(":")
                            orderInfo.create_hour = timeArr(0) //小时
                            orderInfo
                      }
                      // 订单详情表中，字符串转换为样例类对象
                      val orderDetailDStream: DStream[OrderDetail] = inputOrderDetailDstream.map { record =>
                          val jsonString: String = record.value()
                          val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                          orderDetail
                      }
                      // 转化为kv结构
                      val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] =
                        orderInfoDstream.map(orderInfo =>(orderInfo.id,orderInfo))
                      val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] =
                        orderDetailDStream.map(orderDetail=>(orderDetail.order_id,orderDetail))

                      // 双流full outor join
                      val fullOuterjoin: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
                        orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

           //2. 双流join结果处理
           val noUserDStream: DStream[SaleDetail] = fullOuterjoin.mapPartitions(iter => {

                 //获取redis连接
                 val jedisClient: Jedis = RedisUtil.getJedisClient

                 //创建集合用于存放JOIN上(包含当前批次,以及两个流中跟前置批次JOIN上的)的数据
                 val details = new ListBuffer[SaleDetail]
                 //导入样例类对象转换为JSON的隐式
                 implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

                        // 处理分区内的每一条数据
                       iter.foreach { case (orderId, (orderinfoopt, detailOpt)) => {

                           //定义info&detail数据存入Redis中的Key
                           val infoRedisKey = s"order_info:$orderId"
                           val detailRedisKey = s"order_detail:$orderId"
                           //2.1 订单表有值
                           if (orderinfoopt.isDefined) {
                               // 获取取
                               val orderInfo: OrderInfo = orderinfoopt.get
                               //1.1 detailOpt也有值
                               if (detailOpt.isDefined) {
                                   //获取detailOpt中的数据
                                   val orderDetail: OrderDetail = detailOpt.get
                                   //结合放入集合
                                   details += new SaleDetail(orderInfo, orderDetail)
                               }

                               //1.2 将orderInfo转换JSON字符串写入Redis
                               // JSON.toJSONString(orderInfo)  编译报错
                               val orderInfoJson: String = Serialization.write(orderInfo)
                               jedisClient.set(infoRedisKey, orderInfoJson)
                               jedisClient.expire(infoRedisKey, 100)


                               //1.3 查询OrderDetail流前置批次数据
                               val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)


                               orderDetailJsonSet.asScala.foreach(orderDetailJson => {
                                   //将orderDetailJson转换为样例类对象
                                   val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                                   //结合存放入集合
                                   details += new SaleDetail(orderInfo, orderDetail)
                               })


                         } else {
                               //2.infoOpt没有值
                               //获取detailOpt中的数据
                               val orderDetail: OrderDetail = detailOpt.get
                               //查询OrderInfo流前置批次数据
                               val orderInfoJson: String = jedisClient.get(infoRedisKey)
                               if (orderInfoJson != null) {
                                     //2.1 查询有数据
                                     //将orderInfoJson转换为样例类对象
                                     val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                                     //结合写出
                                     details += new SaleDetail(orderInfo, orderDetail)
                               } else {
                                   //2.2 查询没有结果,将当前的orderDetail写入Redis
                                   val orderDetailJson: String = Serialization.write(orderDetail)
                                   jedisClient.sadd(detailRedisKey, orderDetailJson)
                                   jedisClient.expire(detailRedisKey, 100)
                              }
                         }
                       }
                 }
                 // 释放redis连接
                 jedisClient.close()
                 //最终的返回值
                 details.toIterator
           })


    //  3. 根据userid查redis数据，将用户信息补充完整
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(iter => {
          //a. 获取redis连接
          val jedisClient: Jedis = RedisUtil.getJedisClient

          //b. 遍历iter，对每一条数据操作
          val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {

                //查询redis
                val userinforJson: String = jedisClient.get(s"userInfo:${noUserSaleDetail.user_id}")
                //将userinforJson转换为样例类
                val userinfo: UserInfo = JSON.parseObject(userinforJson, classOf[UserInfo])
                //补充信息
                noUserSaleDetail.mergeUserInfo(userinfo)
                //返回数据
                noUserSaleDetail
              })
          //c. 释放连接
          jedisClient.close()
          //d. 返回数据
          details
        })

    // 4. 将三张表的数据写入ES中
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        // 获取当天时间
        val today: String = LocalDate.now().toString()
        val ESINdxe = s"${GmallConstants.GMALL_ES_Salve_Detail_PRE}-$today"

        // 将order_detail_id 作为ES中的docID
        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map { saleDeail => (saleDeail.order_detail_id, saleDeail) }
        // 调用ES工具写入
        MyEsUtil.insertByBulk(ESINdxe,"_doc",detailIdToSaleDetailIter.toList)

      })
    })


    //启动任务
    sparkContext.start()
    sparkContext.awaitTermination()


  }
}
