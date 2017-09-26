package com.uwankeji.spark.structured.sink

import com.uwankeji.spark.structured.entities.Commons.UserEvent
import org.apache.spark.sql.ForeachWriter

/**
 * Created by Administrator on 2017/9/26.
 */
class ConsoleSink extends ForeachWriter[UserEvent]{
  override def open(partitionId: Long, version: Long): Boolean = {
    println("open...")
    true
  }

  override def process(value: UserEvent): Unit = {
    println(s"process:$value")
  }

  override def close(errorOrNull: Throwable): Unit = {
    println("close...")
  }
}
