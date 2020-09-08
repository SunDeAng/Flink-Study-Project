package com.atguigu.loginfail

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Sdaer
 * @Date: 2020-09-08
 * @Desc:
 */
object LoginFailWithCEP {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.读取数据，封装样例类
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[UserLoginEvent] = env.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserLoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: UserLoginEvent): Long = t.timestamp * 1000L
      })

    //2.定义一个匹配模式，用来检测复杂事件序列
    val loginFaulPattern = Pattern
      .begin[UserLoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .next("thirdFail").where(_.eventType == "fail")
      .within(Time.seconds(5))

    //3.对数据流应用定义好的模式，得到PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFaulPattern)

    //4.检测符合匹配条件的事件序列，做出转换输出
    val loginFailDataStream = patternStream.select(new LoginFailSelect())

    //5.打印输出
    loginFailDataStream.print()

    env.execute()
  }
}

class LoginFailSelect() extends PatternSelectFunction[UserLoginEvent,LoginFailWarning]{
  override def select(pattern: util.Map[String, util.List[UserLoginEvent]]): LoginFailWarning = {

    //从map结果中可以拿到第一层和第二层登陆失败的事件
    val firstFailEvent = pattern.get("firstFail").get(0)
    val secondFailEvent = pattern.get("secondFail").get(0)
    val thirdFailEvent = pattern.get("thirdFail").get(0)

    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp,"login fail" )


  }
}