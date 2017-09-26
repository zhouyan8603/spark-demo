package com.uwankeji.spark.structured.entities

import java.io.Serializable
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.fasterxml.jackson.annotation.JsonProperty

import scala.beans.BeanProperty


/**
 * Created by Administrator on 2017/9/26.
 */
object Commons {
  case class UserEvent(
                        @JsonProperty("user_id") user_id: String,
                        @JsonProperty("timestamp") timestamp: String,
                        @JsonProperty("event_id") event_id: String)
    extends Serializable

  def getTimeStamp(timeStr: String): Timestamp = {
    val dateFormat1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(dateFormat1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(dateFormat2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }

}
