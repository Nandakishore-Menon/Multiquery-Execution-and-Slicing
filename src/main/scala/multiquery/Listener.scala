package org.multiquery

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import java.io._


class Listener extends StreamingQueryListener {

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println("start:" + event.timestamp)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
//    println("progress:" + event.progress)
  // to print statistics related to multiquery execution and write them to a file
    val latency = event.progress.durationMs.get("triggerExecution")/1000.0;
    val throughput = event.progress.processedRowsPerSecond
    val inputRows = event.progress.numInputRows
    println("inputRows: " + inputRows);
    println("latency: " + latency);
    println("throughput: " + throughput);
    println()
    val fw = new FileWriter("target/output/bench.txt", true)
    val form = "%d,%f,%f\n"
    val line = form.format(inputRows, latency, throughput)
    fw.write(line)
    fw.close()
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println("term:" + event.id)
  }
}
