package com.uwankeji.spark.structured.main

import com.uwankeji.spark.structured.entities.Commons
import com.uwankeji.spark.structured.service.Deserializer
import com.uwankeji.spark.structured.sink.ConsoleSink
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.sql.{SparkSession}

/**
 * Created by Administrator on 2017/9/26.
 */
object KafkaToSpark {
  private val logger = Logger.getLogger(this.getClass)

  implicit class RichToString(val x: java.nio.ByteBuffer) extends AnyVal {
    def byteArrayToString() = new String( x.array.takeWhile(_ != 0), "UTF-8" )
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.INFO)

    logger.setLevel(Level.INFO)

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.printStackTrace())
    }
  }
}

class SparkJob extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  logger.setLevel(Level.INFO)
  val sparkSession =
    SparkSession.builder
      .master("local[4]")
      .appName("kafka2Spark")
      .getOrCreate()

  // Check this class thoroughly, it does some initializations which shouldn't be in PRODUCTION
  // WARNING: go through this class properly.
  @transient val consoleWriter = new ConsoleSink

  def runJob() = {

    logger.info("Execution started with following configuration")


    val lines = sparkSession.readStream
      .format("kafka")
      .option("subscribe", "test")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("value",
        "CAST(topic as STRING)",
        "CAST(partition as INTEGER)")

    lines.printSchema()

    import sparkSession.implicits._


    lines.select($"value").writeStream.format("console").start()

    val df = lines
      .select($"value")
      .withColumn("deserialized", Deserializer.deser($"value"))
      .select($"deserialized")

    df.printSchema()

    val ds = df
      .select($"deserialized.user_id",
        $"deserialized.timestamp",
        $"deserialized.event_id")
      .as[Commons.UserEvent]

    val query =
      ds.writeStream
        .queryName("kafka2Sparka")
        .foreach(consoleWriter)
        .start()

    sparkSession.streams.awaitAnyTermination()
    sparkSession.stop()
  }
}
