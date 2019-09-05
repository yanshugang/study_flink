package com.study.flink.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.slf4j.LoggerFactory

object LogAnalysis {

  val logger = LoggerFactory.getLogger("LogAnalysis")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收kafka数据
    val topic = "hello_topic"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    //    properties.setProperty("group.id", "test_group")

    val comsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), properties)

    // 接收kafka数据
    import org.apache.flink.api.scala._
    val data = env.addSource(comsumer)

    val log_data = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val time_str = splits(3)
      var time = 0l

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(time_str).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $time_str", e.getMessage)
        }
      }

      val domain = splits(5)
      val traffic = splits(6)

      (level, time, domain, traffic)

    }).filter(_._2 != 0).filter(_._1 == "E") // 根据业务过滤数据
      .map(x => {
      (x._2, x._3, x._4)
    })



    // data.print().setParallelism(1)
    log_data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String)] {

      val maxOutOfOrderness = 10000L
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1).window(TumblingEventTimeWindows.of(Time)).apply(new WindowFunction[(Long, String, Long), (Long, String, Long), Tuple, TImeWindow]:Unit{






    })


    env.execute("LogAnalysis")
  }
}
