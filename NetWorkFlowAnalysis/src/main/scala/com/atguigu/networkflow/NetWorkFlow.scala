package com.atguigu.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author: Sdaer
 * @Date: 2020-09-07
 * @Desc:
 *       网络流量统计分析（乱序数据处理）
 *       一、问题
 *          1、设置
 */
object NetWorkFlow {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置并行度
    env.setParallelism(1)
    //时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    //读取本地文件
    val inputStream: DataStream[String] = env.readTextFile("E:\\Project\\Flink-Project\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(" ")
        //从日志数据中提取时间字段并转换成时间戳
        val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDataFormat.parse(arr(3)).getTime
        WebServerLogEvent(arr(0),arr(1),timestamp,arr(5),arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WebServerLogEvent](Time.minutes(1)) {
        override def extractTimestamp(t: WebServerLogEvent): Long = t.timestamp
      })

    //处理侧输出流的标记
    val lateTag = new OutputTag[WebServerLogEvent]("late")

    //开窗聚合
    val aggStream = dataStream
      .filter(_.method == "GET")  //对GET请求过滤
      .keyBy(_.url)     //按url分组
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.minutes(1))   //允许数据延迟1mn
      .sideOutputLateData(lateTag)  //将延迟1min之后到的数据放入侧输出流
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    dataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(lateTag).print("late")

    //排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)   //若传入的值为"windowEnd",需下面自定义process方式时使用Tuple而不是String
      .process(new TopNPageResult(3))

    resultStream.print()


    env.execute()

  }

}

//输入及窗口聚合结果样例类
case class WebServerLogEvent(ip:String,userId:String, timestamp:Long, method:String, url:String)
case class PageViewCount(url:String, count:Long, windowEnd:Long)

class PageCountAgg() extends AggregateFunction[WebServerLogEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: WebServerLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {

    out.collect(PageViewCount(key, input.head, window.getEnd))

  }
}

//实现自定义ProcessFunction进行排序输出
class TopNPageResult(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{

  //Method1定义一个列表状态ListState
  //lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-ListState",classOf[PageViewCount]))

  //Method2为了对用同一个key进行更新操作，定义映射状态
  lazy val pageViewCountMapState: MapState[String,Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("pageViewCount-map",classOf[String],classOf[Long]))


  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {

    //将数据添加到列表状态
    //pageViewCountListState.add(i)   //M1
    pageViewCountMapState.put(i.url,i.count)  //M2

    //注册定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)
    //定义一个1分钟之后的定时器，用来清理状态
    context.timerService().registerEventTimeTimer(i.windowEnd + 60 * 1000L)

  }

  //触发定时器，对数据进行分组排序
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    if (timestamp == ctx.getCurrentKey + 60 * 1000L){
      pageViewCountMapState.clear()  //到达1分钟，清理状态
      return
    }

    //获取状态中的所有窗口聚合结果,
    // M1 此操作会将所有数据添加到List中，会占用大量内存
//    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
//    val iter = pageViewCountListState.get().iterator()
//    while (iter.hasNext){
//      allPageViewCounts += iter.next()
//    }

    // M2 此操作只会将所有数据中的id及count添加到Map中
    val allPageViewCounts: ListBuffer[(String,Long)] = ListBuffer()
    val iter =pageViewCountMapState.entries().iterator()
    while (iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey,entry.getValue))
    }

    //M1 提取清理状态
//    pageViewCountListState.clear()

    //排序取topN
    //M1排序
    //val topNHotPageViewCounts = allPageViewCounts.sortWith(_.count > _.count).take(n)
    //M2排序
    val topNHotPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)


    //排序格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历TOPN列表，逐个输出
    for (elem <- topNHotPageViewCounts.indices) {
      val currentItemViewCount = topNHotPageViewCounts(elem)
      result.append("NO.").append(elem + 1).append(":")
        .append("\t 页面 URL = ").append( currentItemViewCount._1 )
        .append("\t 热门度 = ").append( currentItemViewCount._2 )
        .append("\n")
    }
    result.append("\n =================================== \n\n")

    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())


  }
}