package com.atguigu.order

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-09
 * @Desc:
 */
object PayMsgMatch {

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

    val recpitResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(recpitResource.getPath)
      .map(line =>{
        val arr = line.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)



    //用connect连接两条流进行处理
    val resultStream = orderEventStream.connect(receiptEventStream)
      .keyBy(_.txId,_.txId)
      .process(new TxMatchDetect())


    resultStream.print("normal")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("single-pay")).print("spay")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("single-receipt")).print("sreceipt")

    env.execute()

  }

}

//到账事件样例类
case class ReceiptEvent(txId:String,payChannel:String,timestamp:Long)



//实现自定义处理函数
class TxMatchDetect() extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent,ReceiptEvent)] {
  //定义状态，用来保存当前已到达的pay事件已经receipts事件
  lazy val payEventState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-value",classOf[OrderEvent]))
  lazy val receiptEventState:ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-value",classOf[ReceiptEvent]))


  override def processElement1(pay: OrderEvent, context: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //来的是pay事件，判断是否有receipts事件到
    val receipt = receiptEventState.value()
    if (receipt != null){
      //已经有到账信息，直接输出到主流
      collector.collect((pay,receipt))
      //清空状态
      payEventState.clear()
      receiptEventState.clear()
    }else{
      //如果没有，更新状态，注册定时器
      payEventState.update(pay)

      context.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)  //等待5s
    }

  }

  override def processElement2(receipt: ReceiptEvent, context: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //来的是receipt事件，判断是否有pay事件到
    val pay = payEventState.value()
    if (pay != null){
      //已经有到账信息，直接输出到主流
      collector.collect((pay,receipt))
      //清空状态
      payEventState.clear()
      receiptEventState.clear()
    }else{
      //如果没有，更新状态，注册定时器
      receiptEventState.update(receipt)

      context.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 5000L)  //等待5s

    }


  }

  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //定时器触发,判断是否有某一个状态不为空，如果不为空，就是另外一个每来
    if (payEventState.value() != null){
      ctx.output(new OutputTag[OrderEvent]("single-pay"),payEventState.value())
    }
    if (receiptEventState.value() != null){
      ctx.output(new OutputTag[ReceiptEvent]("single-receipt"),receiptEventState.value())
    }
    //清空状态
    payEventState.clear()
    receiptEventState.clear()

  }
}