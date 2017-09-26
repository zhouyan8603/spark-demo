package com.uwankeji.spark.structured.main

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.reflect.io.Path

/**
 * Created by Administrator on 2017/9/27.
 */
object KafkaProduceMsg {

  def main(args: Array[String]) {
    run()
  }
  private val BROKER_LIST = "localhost:9092" //"master:9092,worker1:9092,worker2:9092"
  private val TARGET_TOPIC = "test" //"new"
  private val DIR = "C:\\temp\\kafka\\"

  /**
   * 1、配置属性
   * metadata.broker.list : kafka集群的broker，只需指定2个即可
   * serializer.class : 如何序列化发送消息
   * request.required.acks : 1代表需要broker接收到消息后acknowledgment,默认是0
   * producer.type : 默认就是同步sync
   */
  private val props = new Properties()
  props.put("bootstrap.servers", this.BROKER_LIST)
//  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

  /**
   * 2、创建Producer
   */
  private val producer = new KafkaProducer[String, Array[Byte]](props)

  /**
   * 3、产生并发送消息
   * 搜索目录dir下的所有包含“transaction”的文件并将每行的记录以消息的形式发送到kafka
   *
   */
  def run() : Unit = {
    while(true){
      val files = Path(this.DIR).walkFilter(p => p.isFile)

      try{
        for(file <- files){
          val reader = Source.fromFile(file.toString(), "UTF-8")

          for(line <- reader.getLines()){
            val message = new ProducerRecord[String, Array[Byte]](this.TARGET_TOPIC, line.getBytes)
            producer.send(message)
          }

          //produce完成后，将文件copy到另一个目录，之后delete
          val fileName = file.toFile.name
          file.toFile.changeExtension(".completed")
          file.delete()
        }
      }catch{
        case e : Exception => println(e)
      }

      try{
        //sleep for 3 seconds after send a micro batch of message
        Thread.sleep(3000)
      }catch{
        case e : Exception => println(e)
      }
    }
  }
}
