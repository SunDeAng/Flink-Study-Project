package com.atguigu.hotitems


import java.sql.Timestamp
import java.util.Properties

import com.atguigu.beans.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-05
 * @Desc:
 */
object HotItems {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置并行度
    env.setParallelism(1)
    //时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    //读取本地文件
    //val inputStream: DataStream[String] = env.readTextFile("E:\\Project\\Flink-Project\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //读取kafka流数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val inputStream :DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))



    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1),Time.minutes(5))
      //此处的用法为前面聚合的结果传入后面的方法，从后面的方法获取窗口状态
      .aggregate(new ItemCountAgg(), new ItemCountWindowResult())

//    aggStream.print()

    //按照窗口分组，排序输出TOPN
    val resultStream : DataStream[String] = aggStream
        .keyBy("windowEnd")
        .process(new TopNHotItemsResult(5))

    resultStream.print()

    env.execute()


  }



}

//实现自定义预聚合函数
class ItemCountAgg() extends AggregateFunction[UserBehavior,Long, Long]{

  //计数器初始值设置
  override def createAccumulator(): Long = 0L

  //每来一个元素，聚合状态+1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //结果直接输出
  override def getResult(acc: Long): Long = acc

  //将两个状态叠加
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//实现自定义窗口函数
class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  //重新apply方法
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //获取key的值，因为key只有一个，则用此方法获取(由于keyBy里传的字符串，则此处key为Tuple类型)
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    //获取window的结束状态，结束状态时一个时间戳
    //后面按照windowEnd进行标记分组排序，因此会使用windowEnd
    val windowEnd = window.getEnd
    //获取从ItemCountAgg()中聚合的值(getResult的值)，在此处是迭代器
    //因为传来的只有一个值，因此用迭代器直接获取
    val count = input.iterator.next()
    //将key，聚合值，窗口结束状态转为样例类输出
    out.collect(ItemViewCount(itemId,count,windowEnd))
  }
}

//实现自定义的keyedProcessFunction
class TopNHotItemsResult(n :Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

  //定义一个列表状态 用来保存当前窗口内所有商品的count统计结果
  private var itemViewCountListState: ListState[ItemViewCount] = _

  //给列表状态赋予初始值(此为标准的状态设置)
  override def open(parameters: Configuration): Unit = {

    itemViewCountListState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("itemViewCount-List",classOf[ItemViewCount])
    )

  }

  //流程处理：每来一个数据，就把他加入状态
  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

    itemViewCountListState.add(i)
    //注册定时器 windowEnd+100ms触发
    //按照时间戳触发，每个窗口的时间戳一样，因此不用判断是否有定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)

  }

  //定时器触发，窗口的所有统计结果都到齐，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //先将状态提取，用一个list保存
    //引入隐式转换成list的包
    import  scala.collection.JavaConversions._
    val allItemViewCountList: List[ItemViewCount] = itemViewCountListState.get().toList

    //提取清空状态
    itemViewCountListState.clear()

    //按照点击量从大到小排序，并取TopN
    val topNHotItemViewCountList = allItemViewCountList
      .sortBy(_.count)(Ordering.Long.reverse)
      .take(n)

    //排名信息格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp-100)).append("\n")
    //遍历TopN列表，逐个输出
    for (i <- topNHotItemViewCountList.indices){
      val currentItemViewCount = topNHotItemViewCountList(i)
      result.append("NO.").append(i + 1).append(": ")
        .append("\t 商品ID = ").append(currentItemViewCount.itemId)
        .append("\t 热门度 = ").append(currentItemViewCount.count)
        .append("\n")
    }
    result.append("\n ================================= \n\n")

    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())

  }
}



