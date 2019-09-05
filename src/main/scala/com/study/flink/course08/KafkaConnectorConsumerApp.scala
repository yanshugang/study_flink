package com.study.flink.course08

/**
  * 使用kafka作为source
  */

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object KafkaConnectorConsumerApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val topic = "test_topic"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val data = env.addSource(new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), properties))
    data.print()


    env.execute("KafkaConnectorConsumerApp")
  }
}
