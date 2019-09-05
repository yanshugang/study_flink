package com.study.flink.project


import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random


/**
  * 编写一个日志模拟器，作为生产者向kafka中发送数据。
  */
object KafkaProducer {


  def get_levels(): String = {
    val levels = Array[String]("M", "E")
    levels.toVector(new Random().nextInt(levels.length))
  }


  def get_ips(): String = {
    val ips = Array[String](
      "180.104.63.120",
      "114.235.23.251",
      "121.13.252.60",
      "115.53.37.169",
      "60.13.42.241",
      "183.129.207.80",
      "58.22.204.62",
      "114.215.139.19",
      "115.211.33.130",
      "114.235.23.195",
      "117.90.137.181",
      "125.110.89.163",
      "47.98.174.153",
      "1.197.203.93"
    )

    ips.toVector(new Random().nextInt(ips.length))
  }

  def get_domains(): String = {
    val domains = Array[String](
      "v1.flink.org",
      "v2.flink.org",
      "v3.flink.org",
      "v4.flink.org",
      "v5.flink.org"
    )
    domains.toVector(new Random().nextInt(domains.length))
  }

  def get_tracfics(): Long = {

    new Random().nextInt(10000)

  }

  def main(args: Array[String]): Unit = {

    // val brokers = ""


    val properties = new Properties()
    properties.setProperty("metadata.broker.list", "localhost:9092")
    properties.setProperty("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(properties)
    val producer = new Producer[String, String](config)

    val topic = "hello_topic"

    // 通过死循环一直不停的往kafka的broker中生产数据
    while (true) {

      // mock data
      val builder = new StringBuilder()
      builder.append("flink").append("\t")
        .append("CN").append("\t") // 地区
        .append(get_levels()).append("\t") // 级别
        .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t") // 时间
        .append(get_ips()).append("\t") // ip
        .append(get_domains()).append("\t") // 域名
        .append(get_tracfics()).append("\t") // 流量

      println(builder)

      // send data to kafka
      val data = new KeyedMessage[String, String](topic, builder.toString())
      producer.send(data)

      Thread.sleep(2000)
    }
  }

}


// ./kafka-console-consumer.sh --zookeeper localhost:2181 --topic hello_topic