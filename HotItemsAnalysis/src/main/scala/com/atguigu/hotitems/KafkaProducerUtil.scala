package com.atguigu.hotitems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Author: Sdaer
 * @Date: 2020-09-05
 * @Desc:
 */
object KafkaProducerUtil {

  def main(args: Array[String]): Unit = {

    writeToKafka("hotitems")

  }

  def writeToKafka(topic: String): Unit ={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    val producer = new KafkaProducer[String,String](properties)

    //从文件读取数据
    val bufferedSource = io.Source.fromFile("E:\\Project\\Flink-Project\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (elem <- bufferedSource.getLines()) {

      val record = new ProducerRecord[String,String](topic,elem)
      producer.send(record)

    }
    producer.close()

  }

}
