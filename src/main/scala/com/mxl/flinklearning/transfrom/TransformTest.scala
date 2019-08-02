package com.mxl.flinklearning.transfrom

import com.mxl.flinklearning.source.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文件读取数据
    val fileTextStream: DataStream[String] = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\flink-learning\\src\\main\\resources\\sensor.txt")
    val sensorStream: DataStream[SensorReading] = fileTextStream.map(line => {
      val arr = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //filter
    sensorStream.filter(_.temperature > 30d).print("filter")

    //keyBy
    val keyedStream: KeyedStream[SensorReading, Tuple] = sensorStream.keyBy("id")

    //reduce
    val reduceStream: DataStream[SensorReading] = keyedStream.reduce((x, y) => {
      SensorReading(x.id, y.timeStamp + 1, y.temperature)
    })

    //splict & select
    val splitStream: SplitStream[SensorReading] = sensorStream.split(reading => if (reading.temperature > 30d) Seq("hign") else Seq("low"))
    val hignStream: DataStream[SensorReading] = splitStream.select("hign")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    hignStream.print("hign")
    lowStream.print("low")

    //connect & coMap,coFlatMap
    val connectedStream: ConnectedStreams[SensorReading, SensorReading] = hignStream.connect(lowStream)
    connectedStream.map(
      x => (x.id, x.temperature, "warning"),
      y => (y.id, y.temperature, "normal")
    ).print("connect & coMap")

    //union
    val unionStream: DataStream[SensorReading] = hignStream.union(lowStream)

    env.execute("transform test stream")
  }
}
