package com.github.robertwsmith.dataquality.spark.ml.params

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.json4s.JsonAST.{JDouble, JObject, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class StringDoubleMapParam(
  parent: String,
  name: String,
  doc: String,
  isValid: Map[String, Double] => Boolean
) extends Param[Map[String, Double]](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, (_: Map[String, Double]) => true)

  def this(
    parent: Identifiable,
    name: String,
    doc: String,
    isValid: Map[String, Double] => Boolean
  ) = this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (_: Map[String, Double]) => true)

  override def jsonEncode(value: Map[String, Double]): String = {
    val json: List[(String, JValue)] = value.map { v => v._1 -> JDouble(v._2) }.toList
    compact(render(JObject(json)))
  }

  override def jsonDecode(json: String): Map[String, Double] = {
    parse(json) match {
      case JObject(obj) =>
        obj.collect {
          case (lhs, JDouble(rhs)) => lhs -> rhs
          case _                   => throw new Exception(s"Malformed JSON: \n${json}")
        }.toMap
      case _ => throw new Exception(s"Malformed JSON: \n${json}")
    }
  }
}
