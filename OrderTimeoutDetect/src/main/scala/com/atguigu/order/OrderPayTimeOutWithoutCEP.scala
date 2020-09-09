package com.atguigu.order

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-09
 * @Desc:
 */
object OrderPayTimeOutWithoutCEP {

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

    //自定义ProcessFunction，检测不同订单支付状态
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayDetect())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("data not fund")).print("NoCreate")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("payed-but-timeout")).print("payed-but-timeout")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("timeout")).print("timeout")

    env.execute()

  }

}

class OrderPayDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderPayResult] {
  //定义状态用来保存是否来过create和pay事件,保存定时器时间戳
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed",classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreated",classOf[Boolean]))
  lazy val timestampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts",classOf[Long]))


  override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#Context, collector: Collector[OrderPayResult]): Unit = {
    //先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timestampState.value()

    //判断当前事件类型
    if (i.eventType == "create"){
      //1如果来的是创建，判断之前是否支付过
      if (isPayed){
        //如果已经支付过，匹配成功，输出到主流
        collector.collect(OrderPayResult(i.orderId,"payed successfully"))
        //清除定时器，清空状态
        context.timerService().deleteEventTimeTimer(timerTs)
        isCreatedState.clear()
        isPayedState.clear()
        timestampState.clear()
      }else{
        //如果pay没来过，最正常状况，注册15分组定时器，开始等待
        val ts = i.timestamp *1000L + 15* 60 * 1000L
        context.timerService().registerEventTimeTimer(ts)
        //更新状态
        timestampState.update(ts)
        isCreatedState.update(true)
      }
    }else if(i.eventType == "pay"){
      //来的是pay，判断是否create过
      if (isCreated){
        //已经create过，匹配成，还要再确定pay时间是否超过了定时器时间
        if (i.timestamp * 1000L < timerTs){
          //没超时，正常输出到主流
          collector.collect(OrderPayResult(i.orderId,"payed successfully"))
        }else{
          //已经超时，因为乱序数据到来，定时器没触发，输出到侧输出流
          context.output(new OutputTag[OrderPayResult]("payed-but-timeout"),OrderPayResult(i.orderId,"payed-but-timeout"))
        }
        //已经处理完当前订单状态，清空状态
        isCreatedState.clear()
        isPayedState.clear()
        timestampState.clear()
      }else{
        //如果没有created过，乱序，注册定时器等待created
        context.timerService().registerEventTimeTimer(i.timestamp * 1000L)
        //更新状态
        timestampState.update(i.timestamp * 1000L)
        isPayedState.update(true)
      }

    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#OnTimerContext, out: Collector[OrderPayResult]): Unit = {

    //定时器触发，说明pay和create有一个每来
    if (isPayedState.value()){
      ctx.output(new OutputTag[OrderPayResult]("data not fund"),OrderPayResult(ctx.getCurrentKey,"payed not find created"))

    }else{
      //没有pay过，正在超时
      ctx.output(new OutputTag[OrderPayResult]("timeout"),OrderPayResult(ctx.getCurrentKey,"timeout"))
    }

  }
}