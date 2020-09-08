package com.atguigu.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.api.java.tuple.{Tuple,Tuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Author: Sdaer
 * @Date: 2020-09-08
 * @Desc:
 *      需求：根据渠道统计行为数据（如微信   下载    10次）
 *      流程：
 *        1.设置环境，时间语义（观察升序或乱序）
 *        2.获取数据流，过滤，分组（使用组合Key），开窗，处理（使用底层process方法）
 *
 */
object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream : DataStream[MarketingUserBehavior] = env.addSource(new MarketingSumulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)


    val resultStream = dataStream
      .filter(_.behavior != "UNINSTALL")  //过滤掉卸载行为
      .keyBy("channel", "behavior") //按照渠道和行为分组
      //.keyBy(data => (data.channel,data.behavior))  M2
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketingCountByChannel())

    resultStream.print()

    env.execute()
  }

}

//输入数据的样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel:String, timestamp: Long)
//输出统计数据的样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String)

//自定义测试数据源
class MarketingSumulatedEventSource extends RichSourceFunction[MarketingUserBehavior]{

  //是否正常运行的标志位
  var running = true
  //定义用户行为及渠道的集合
  val behaviorSet:Seq[String] = Seq("CLICK", "DOWLOAD", "INSTALL", "UNINSTALL")
  val channelSet:Seq[String] = Seq("AppStore", "HuaweiStore", "Wechat","Weibo")


  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit ={

    //定义生成数据上限
    val maxElements = Long.MaxValue
    var count = 0L

    while (running && count < maxElements){
      //所有字段随机生成  此处没做权重随机
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(Random.nextInt(behaviorSet.size))
      val channel = channelSet(Random.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      sourceContext.collect(MarketingUserBehavior(id, behavior, channel, ts))

      count += 1

      Thread.sleep(10)

    }

  }

  override def cancel(): Unit = running = false
}

//自定义分组统计
class MarketingCountByChannel() extends ProcessWindowFunction[MarketingUserBehavior,MarketingViewCount,Tuple, TimeWindow]{
//class MarketingCountByChannel() extends ProcessWindowFunction[MarketingUserBehavior,MarketingViewCount,(String, String), TimeWindow]{
  override def process(key: Tuple, context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key.asInstanceOf[Tuple2[String,String]].f0
    val behavior = key.asInstanceOf[Tuple2[String,String]].f1
//    val channel = key._1
//    val behavior = key._2
    val count = elements.size
    out.collect(MarketingViewCount(start,end,channel,behavior))

  }
}