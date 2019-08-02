package com.mxl.flinklearning.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合读取数据
    val stream: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 2, 20.8214),
      SensorReading("sensor_2", 2, 20.8214),
      SensorReading("sensor_3", 2, 20.8214),
      SensorReading("sensor_4", 2, 20.8214)))
    stream.print("collectionStream").setParallelism(1)

    //从文件读取数据
    val fileTextStream: DataStream[String] = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\flink-learning\\src\\main\\resources\\sensor.txt")
    fileTextStream.map(line => {
      val arr = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .print("fileStream").setParallelism(1)

    // 以kafka消息队列的数据作为来源
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.213.101:9092,192.168.213.102:9092,192.168.213.103:9092")
    properties.setProperty("group.id", "test")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //    env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    //      .print("kafkaStream")
    //      .setParallelism(1)

    env.execute("Stream")
  }
}
