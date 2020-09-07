package com.atguigu.hotitems

import com.atguigu.beans.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @Author: Sdaer
 * @Date: 2020-09-05
 * @Desc:
 */
object HotItemsWithTableAPI {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置并行度
    env.setParallelism(1)
    //时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    //读取本地文件
    val inputStream: DataStream[String] = env.readTextFile("E:\\Project\\Flink-Project\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")


    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //表环境的创建
    val setting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, setting)

      //将DS转化为表
      val dataTable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //基于Table API进行窗口聚合
    val aggTable: Table = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)

    //aggTable.toAppendStream[Row].print("agg")

    //SQL排序输出
    tableEnv.createTemporaryView("agg", aggTable,'itemId, 'cnt, 'windowEnd)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from(
        | select *,
        |   row_number() over (partition by windowEnd order by cnt desc) as row_num
        | from agg
        |)
        |where row_num <= 5
        |""".stripMargin)

    resultSqlTable.toRetractStream[Row].print()

    env.execute()

  }

}
