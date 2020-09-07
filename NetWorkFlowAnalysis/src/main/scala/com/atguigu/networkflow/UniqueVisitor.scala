package com.atguigu.networkflow

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Sdaer
 * @Date: 2020-09-07
 * @Desc: 计算UV
 *       使用set数据结构去重，小规模使用
 *       使用redis中的set去重，中等规模使用
 *       使用布隆过滤器，大规模数据使用
 */
object UniqueVisitor {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置并行度
    env.setParallelism(1)
    //时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    //读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
//      .apply(new UvCountResult()) //M1
        .aggregate(new UvCountAgg(), new UvCountAggResult())  //M2

    uvStream.print()

    env.execute()

  }

}

case class UvCount(windowEnd: Long, count: Long)

//实现自定义WindowFunction
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    //使用一个set集合，来保存所有的userID，自动去重
    var idSet = Set[Long]()
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
      //输出set的大小
      out.collect(UvCount(window.getEnd, idSet.size))
  }
}

//M2
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

class UvCountAggResult() extends AllWindowFunction[Long, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {

    out.collect(UvCount(window.getEnd, input.head))

  }
}