package com.mxl.flinklearning

import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("D:\\Documents\\mrinput\\wordcount")

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    counts.print().setParallelism(1)

    env.execute("Streaming WordCount")
  }
}
