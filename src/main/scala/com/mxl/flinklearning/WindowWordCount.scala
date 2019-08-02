package com.mxl.flinklearning

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text =
      if (params.has("input")) {
        // read the text file from given input path
        env.readTextFile(params.get("input"))
      } else {
        //println("Executing WindowWordCount example with default input data set.")
        //println("Use --input to specify file input.")
        // get default test text data
        env.readTextFile("D:\\Documents\\mrinput\\wordcount")
      }

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val windowSize = params.getInt("window", 10)
    val slideSize = params.getInt("slide", 5)

    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // create windows of windowSize records slided every slideSize records
      .countWindow(windowSize, slideSize)
      // group by the tuple field "0" and sum up tuple field "1"
      .sum(1)


    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print().setParallelism(1)
    }

    env.execute("Streaming WordCount")
  }
}
