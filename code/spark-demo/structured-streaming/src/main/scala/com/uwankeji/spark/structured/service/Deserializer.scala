package com.uwankeji.spark.structured.service


import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.google.gson.Gson
import com.uwankeji.spark.structured.entities.Commons
import com.uwankeji.spark.structured.entities.Commons.UserEvent
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.functions.udf

import scala.io.Source

/**
 * Created by Administrator on 2017/9/26.
 */
object Deserializer extends Serializable{
  val deser = udf { (input: Array[Byte]) =>
    deserializeMessage(input)
  }
  //read avro schema file
  @transient lazy val schemaString =
    Source.fromURL(getClass.getResource("/message.avsc")).mkString
  // Initialize schema
  @transient lazy val schema: Schema =
    new Schema.Parser().parse(schemaString)
  @transient lazy val gson = new Gson()

  private def deserializeMessage(input: Array[Byte]): Commons.UserEvent = {
    try {

      val in = new ByteArrayInputStream(input)
      val msg = gson.fromJson(IOUtils.toString(in, StandardCharsets.UTF_8),classOf[UserEvent])
      msg
    } catch {
      case e: Exception => null
    }
  }
}
