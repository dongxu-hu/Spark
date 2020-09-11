package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object SaveUserInfoToRedisApp {

  def main(args: Array[String]): Unit = {

    // 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaveUserInfoToRedisApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    // 读取kafka中user数据
    val UserDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_USER_INFO, ssc)

    UserDStream.foreachRDD(rdd=>{

      //对分区操作,减少连接的创建与释放
      rdd.mapPartitions(iter=>{
        //创建redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //遍历iter,写入Redis
        iter.foreach(reconrd=>{
          val userInfoJson: String = reconrd.value()
          val userInfor: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          //定义redis中key的值
          val userinfoID = s"userInfo: ${userInfor.id}"
          //写入redis
          jedisClient.set(userinfoID, userInfoJson)
        })

      jedisClient.close()
      })
    })



    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
