package com.atguigu.order

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-09
 * @Desc:
 *       此方式的实现类似于Spark实时项目中最难的那个需求
 *       此种具体调用需反复研究
 *
 */
object PayMsgWithJoin {

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
      .filter(_.txId != "")
      .keyBy(_.txId)

    val recpitResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(recpitResource.getPath)
      .map(line =>{
        val arr = line.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    val resultStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3),Time.seconds(5))  //对-3~5直接的数据进行匹配
      .process(new TxMatcjDetectWithJoin())

    resultStream.print()
    env.execute()

  }

}

class TxMatcjDetectWithJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    collector.collect((in1,in2))

  }
}