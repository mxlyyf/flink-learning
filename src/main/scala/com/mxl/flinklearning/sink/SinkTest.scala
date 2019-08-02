package com.mxl.flinklearning.sink

import com.mxl.flinklearning.source.SensorReading
import org.apache.flink.streaming.api.scala._

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文件读取数据
    val fileTextStream: DataStream[String] = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\flink-learning\\src\\main\\resources\\sensor.txt")
    val sensorStream: DataStream[String] = fileTextStream.map(line => {
      val arr = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
    })
    sensorStream.print

    env.execute("Sink Test Stream")
  }
}
