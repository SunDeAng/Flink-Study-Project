package com.atguigu.order

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @Author: Sdaer
 * @Date: 2020-09-09
 * @Desc:
 */
object OrderPayTimeout {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(line =>{
        val arr = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //定义匹配模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //2.将模式应用在数据流上，进行复杂时间序列的检测
    val patternStream = CEP.pattern(orderEventStream.keyBy(_.orderId),orderPayPattern)

    //3.定义侧输出流标签，用于将超时事件输出
    val timeoutOutPutTag =new OutputTag[OrderPayResult]("timeout")

    //4.检出复杂事件并转换输出结果
    val resultStream = patternStream.select(timeoutOutPutTag,new PayTimeout(), new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(timeoutOutPutTag).print("timeout")
    env.execute()


  }

}

case class OrderEvent(orderId:Long, eventType:String, txId:String, timestamp:Long)

case class OrderPayResult(orderId:Long, resultMsg:String)

//自定义patternFuncation
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderPayResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {

    val payedOrderId = map.get("pay").get(0).orderId
    OrderPayResult(payedOrderId,s"订单${payedOrderId}已经支付")

  }
}

class PayTimeout() extends PatternTimeoutFunction[OrderEvent,OrderPayResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderPayResult = {

    val timeoutOrderId = map.get("create").get(0).orderId
    OrderPayResult(timeoutOrderId,s"订单${timeoutOrderId}已经延迟支付")
  }
}