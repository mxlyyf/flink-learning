package com.mxl.flinklearning.udf

import com.mxl.flinklearning.source.SensorReading
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object UDFTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文件读取数据
    val fileTextStream: DataStream[String] = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\flink-learning\\src\\main\\resources\\sensor.txt")
    val sensorStream: DataStream[SensorReading] = fileTextStream.map(line => {
      val arr = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //MyFilter
    sensorStream.filter(new MyFilter).print("filter1")

    sensorStream.filter(new FilterFunction[SensorReading] {
      override def filter(value: SensorReading): Boolean = {
        value.id.contains("sensor")
      }
    }).print("filter2")

    env.execute("UDFTest Stream")
  }
}
