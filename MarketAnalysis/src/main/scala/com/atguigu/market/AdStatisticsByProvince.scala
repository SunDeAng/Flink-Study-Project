package com.atguigu.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-08
 * @Desc:
 *       需求：统计用户点击某个广告次数，进行风控
 *
 *
 *
 */
object AdStatisticsByProvince {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据并转化为样例类
    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map(line =>{
        val arr = line.split(",")
        AdClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //自定义过滤过程，将超出点击上限的用户输出到侧输出流
    val filterStream : DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId,data.adId)) //以用户和广告id分组
      .process(new FilterBlackListUser(100))

    //根据省份分组，开窗聚合
    val adCountStream: DataStream[AdViewCountByProvince] = filterStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("adCount")
    filterStream.getSideOutput(new OutputTag[AdBlackWarning]("black-list")).print("black")

    env.execute()

  }

}

//输入样例类
case class AdClickLog(userId:Long, adId: Long,province:String,city:String,timestamp:Long)
//输出样例类
case class AdViewCountByProvince(windowEnd:String, province:String, count:Long)
//侧输出流黑名单样例类
case class AdBlackWarning(userId: Long, adId:Long, msg: String)

class AdCountAgg() extends AggregateFunction[AdClickLog,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdViewCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCountByProvince]): Unit = {

    val end = new Timestamp(window.getEnd).toString
    out.collect(AdViewCountByProvince(end, key, input.head))

  }
}

//实现自定义的KeyedprocessFunction
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog,AdClickLog]{

  //定义状态，保存当前用户对当前广告的点击count值,0点清空状态定时器时间戳，是否输出到黑名单的标志位
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
  lazy val resetTimerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count1",classOf[Long]))
  lazy val isInBlackListState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isBlack",classOf[Boolean]))


  override def processElement(i: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {

    //获取count值
    val curCount = countState.value()

    //判断是否是当天的第一个数据，如果是，注册第二天0点定时器
    if (curCount == 0){
      val ts = (context.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24) - (1000*60*60*8)
      context.timerService().registerProcessingTimeTimer(ts)
      resetTimerTsState.update(ts)
    }

    //判断是否达到上限，如果达到加入黑名单
    if (curCount >= maxCount){
      //如果没在黑名单里，那么输出到侧输出流黑名单信息中
      if (!isInBlackListState.value()){
        context.output(new OutputTag[AdBlackWarning]("black-list"),AdBlackWarning(i.userId,i.adId,s"超过点击上限"))
        isInBlackListState.update(true)
      }
      return
    }
    collector.collect(i)
    countState.update(curCount + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

    if (timestamp == resetTimerTsState.value()){

      countState.clear()
      resetTimerTsState.clear()
      isInBlackListState.clear()

    }

  }
}
