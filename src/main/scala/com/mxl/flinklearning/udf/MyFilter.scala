package com.mxl.flinklearning.udf

import com.mxl.flinklearning.source.SensorReading
import org.apache.flink.api.common.functions.FilterFunction

class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.contains("temprature")
  }
}
