package com.study.flink.course05_DataStream

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long] {

  var count = 1L
  var is_running = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (is_running) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    is_running = false
  }
}
