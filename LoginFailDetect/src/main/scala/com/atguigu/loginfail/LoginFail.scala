package com.atguigu.loginfail

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-08
 * @Desc:
 */
object LoginFail {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置并行度
    env.setParallelism(1)
    //时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读数据，封装样例类
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserLoginEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: UserLoginEvent): Long = t.timestamp * 1000L
      })

    //用processFunction实现连续登陆失败检测
    val LoginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailDetectWarning(2))

//    loginEventStream.print()
    LoginFailWarningStream.print()
    env.execute()

  }

}

case class UserLoginEvent(userId:Long, ip:String, eventType:String, timestamp:Long)
case class LoginFailWarning(userId:Long, firstFailTime:Long, lastFailTime:Long, msg:String)

class LoginFailDetectWarning(loginNum: Int) extends KeyedProcessFunction[Long, UserLoginEvent,LoginFailWarning]{

  //定义状态，保存2秒内所有登陆失败事件的列表，定时器时间戳
  lazy val loginFailEventListState:ListState[UserLoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("count",classOf[UserLoginEvent]))
  lazy val timerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count1",classOf[Long]))

  override def processElement(i: UserLoginEvent, context: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {

    //判断当前数据是登陆成功还是失败
    if (i.eventType == "fail"){
      //1.如果失败，把数据添加到列表中，如果没定时器，就添加定时器
      loginFailEventListState.add(i)
      //没定时器，注册
      if (timerTsState == 0){
        val ts = i.timestamp * 1000L + 5000L
        context.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    }else {
      //如果成功，删除定时器，清空状态，重新开始
      context.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailEventListState.clear()
      timerTsState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {

    //报警事件处理
    //定时器触发，说明没有登陆成功数据，要判断一共有多少次登陆失败
    import scala.collection.JavaConversions._
    if (loginFailEventListState.get().size >= loginNum){

      //超过失败上限，输出报警信息
      out.collect(LoginFailWarning(
        ctx.getCurrentKey,
        loginFailEventListState.get().head.timestamp,
        loginFailEventListState.get().last.timestamp,
        s"login in 2s "))
    }
      //清空状态
    loginFailEventListState.clear()
    timerTsState.clear()

  }
}