package com.mxl.flinklearning

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object SocketWindowWordCount {
	def main(args: Array[String]): Unit = {
		// the host and the port to connect to
		var hostname: String = "192.168.213.101"
		var port = 9999

		try {
			val params = ParameterTool.fromArgs(args)
			hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
			port = params.getInt("port")
		} catch {
			case e: Exception => {
				System.err.println("No port specified. Please run 'SocketWindowWordCount " +
					"--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
					"is the address of the text server")
				System.err.println("To start a simple text server, run 'netcat -l <port>' " +
					"and type the input text into the command line")
				return
			}
		}

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		val text = env.socketTextStream(hostname, port)

		val counts = text.flatMap(_.split("\\W+"))
			.filter(_.nonEmpty)
			.map((_, 1))
			.keyBy(0)
			.sum(1)

		counts.print().setParallelism(2)

		env.execute

	}
}
