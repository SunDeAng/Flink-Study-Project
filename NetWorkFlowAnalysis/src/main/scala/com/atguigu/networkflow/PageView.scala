package com.atguigu.networkflow

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Author: Sdaer
 * @Date: 2020-09-07
 * @Desc: 统计PV
 *       通用步骤
 *        0.设置环境
 *        1.读取数据(时间正序，时间乱序此处为涉及)
 *        2.进行格式转换，封装成样例类，设置事件时间
 *        3.开窗统计
 *        4.对开窗统计的数据进行聚合
 *       两种方法
 *        M1  开窗时使用一个key，造成数据倾斜
 *        M2  开窗时使用自定义key分配器，把key打散，防止数据倾斜
 */
object PageView {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置并行度
    env.setParallelism(1)
    //时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    //读取本地文件
    //val inputStream: DataStream[String] = env.readTextFile("E:\\Project\\Flink-Project\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


    //开窗统计
    val pvCountStream = dataStream
      .filter(_.behavior == "pv")
      //M1此分组会造成数据倾斜
      //.map(data => ("pv",1L)) //M1注意传入的时1L不是1，否则会报错，因为处理元祖时使用的时Long
      //.keyBy(_._1)  //M1所有数据都分到一组
      .map(new MyMapper())  //M2自定义key解决数据倾斜，自定义map函数，随机生成key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountResult())

    //将窗口内所有分组数据汇总起来
    val pvTotalStream =  pvCountStream
        .keyBy(_.windowEnd)
        //.sum("count")   //M1数据倾斜
        .process(new TotalPvCountResult())  //M2 将所有key相应的值进行聚合


    pvTotalStream.print()

    env.execute()


  }

}

//定义输入输出样例类
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

//自定义聚合Pv计算
class PvCountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {

    //因为传入的只有一个数，则使用input迭代器的头就可以
    out.collect(PvCount(window.getEnd, input.head))

  }
}

class MyMapper() extends MapFunction[UserBehavior,(String,Long)]{
  //将key打散
  override def map(value: UserBehavior): (String, Long) = (Random.nextString(10), 1L)
}

// 实现自定义的KeyedProcessFunction，实现所有分组数据的聚合叠加
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {

  //定义一个状态，保存当前所有key 的count总和
  lazy val currentTotalCountState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("Pvcount-state",classOf[Long]))


  override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {

    //获取当前count总和
    val currentTotalCount = currentTotalCountState.value()
    //叠加当前数据的count值，更新状态
    currentTotalCountState.update(currentTotalCount + i.count)
    //注册定时器，100ms后触发
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {

    //所有key的count值都几经聚合，直接输出结果
    out.collect(PvCount(ctx.getCurrentKey, currentTotalCountState.value()))
    //清空状态
    currentTotalCountState.clear()

  }
}