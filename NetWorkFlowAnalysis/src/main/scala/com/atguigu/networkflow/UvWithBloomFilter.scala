package com.atguigu.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @Author: Sdaer
 * @Date: 2020-09-07
 * @Desc:
 *       使用布隆过滤器计算cv
 */
object UvWithBloomFilter {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //自定义窗口触发规则
      .process(new UvCountResultWithBloom())

    uvStream.print()
    env.execute()

  }
}

//实现自定义触发器，每个数据来了之后都触发一此窗口计算
class MyTrigger() extends Trigger[(String,Long), TimeWindow] {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

//实现自定义布隆过滤器
class MyBloomFilter(size: Long ) extends Serializable{

  //指定布隆过滤器的位图大小有外部参数指定（2的整次幂），位图放入redis中
  private val cap = size

  //实现一个hash函数
  def hash(value: String, seed : Int): Long = {

    var result = 0L
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    //返回cap范围内的hash值,可以使得hash值落入cap的范围内
    (cap - 1) & result

  }

}

//自定义Process Function，实现对于每个userId的去重判断
class UvCountResultWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String, TimeWindow]{

  //创建redis连接和布隆过滤器
  lazy val jedis = new Jedis("hadoop102", 6397)
  lazy val bloom = new MyBloomFilter(1 << 29) //1亿id，1亿个位大约需要2^27,设置大小为2^29,四百的数量防止hash碰撞

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //每来一个数据，就将他的userId进行hash计算，到redis位图中判断是否存在
    val bitmapKey = context.window.getEnd.toString  //一windowEnd作为位图的key

    val userId = elements.last._2.toString
    val offset = bloom.hash(userId,61)  //调用hash函数，计算位图中偏移量
    val isExist = jedis.getbit(bitmapKey, offset) //调用redis指令，得到是否存在结果

    //如果存在，什么都不做，如果不存在，将对应位置设为1，count值+1
    //因为窗口状态要清空，所以将count值保存到redis中
    val countMap = "uvCount"  //所有窗口的uv count值保存成一个hash map
    val uvCountKey = bitmapKey  //每个窗口的uv count key就是windowEnd
    var count = 0L
    //先取出redis中的count状态
    if (jedis.hget(countMap, uvCountKey) != null){
      count = jedis.hget(countMap,uvCountKey).toLong
    }

    if (!isExist){
      jedis.setbit(bitmapKey, offset, true)
      //更新窗口的uv count统计值
      jedis.hset(countMap, uvCountKey, (count + 1).toString)
      out.collect(UvCount(uvCountKey.toLong,count + 1))
    }
  }
}