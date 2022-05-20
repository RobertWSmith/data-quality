package com.github.robertwsmith.dataquality.spark.ml.params

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class StringMapParam(
  parent: String,
  name: String,
  doc: String,
  isValid: Map[String, String] => Boolean
) extends Param[Map[String, String]](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, (_: Map[String, String]) => true)

  def this(
    parent: Identifiable,
    name: String,
    doc: String,
    isValid: Map[String, String] => Boolean
  ) = this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (_: Map[String, String]) => true)

  override def jsonEncode(value: Map[String, String]): String = {
    val json: List[(String, JValue)] = value.map { v => v._1 -> JString(v._2) }.toList
    compact(render(JObject(json)))
  }

  override def jsonDecode(json: String): Map[String, String] = {
    parse(json) match {
      case JObject(obj) =>
        obj.collect {
          case (lhs, JString(rhs)) => lhs -> rhs
          case _                   => throw new Exception(s"Malformed JSON: \n${json}")
        }.toMap
      case _ => throw new Exception(s"Malformed JSON: \n${json}")
    }
  }
}
