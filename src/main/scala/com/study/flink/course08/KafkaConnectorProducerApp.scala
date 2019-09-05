package com.study.flink.course08

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

/**
  * 使用kafka作为sink
  */
object KafkaConnectorProducerApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从socket接收数据，通过flink，将数据sink到kafka
    val data = env.socketTextStream("localhost", 9999)

    val topic = "test_topic"
    val kafka_sink = new FlinkKafkaProducer09[String]("localhost:9092", topic, new SimpleStringSchema)


    data.addSink(kafka_sink)

    env.execute("KafkaConnectorProducerApp")


  }
}
