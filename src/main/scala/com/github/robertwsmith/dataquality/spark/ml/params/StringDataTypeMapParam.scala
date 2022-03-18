package com.github.robertwsmith.dataquality.spark.ml.params

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, NullType}
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.JObject

class StringDataTypeMapParam(
  parent: String,
  name: String,
  doc: String,
  isValid: Map[String, DataType] => Boolean = _ => true
) extends Param[Map[String, DataType]](parent, name, doc, isValid) {

  def this(
    parent: Identifiable,
    name: String,
    doc: String,
    isValid: Map[String, DataType] => Boolean
  ) = this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (_: Map[String, DataType]) => true)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc)

  override def jsonEncode(value: Map[String, DataType]): String = {
    val pairs = value.collect { case (n, dt) =>
      n -> JString(dt.typeName)
    }.toList
    compact(render(JObject(pairs)))
  }

  override def jsonDecode(json: String): Map[String, DataType] = {
    parse(json) match {
      case JObject(obj) =>
        if (obj.nonEmpty)
          obj.collect { case (n: String, typeName: JValue) =>
            try {
              val JString(name) = typeName
              n -> DataType.fromJson(compact(render(typeName)))
            } catch {
              case i: IllegalArgumentException => n -> NullType
            }
          }.toMap
        else {
          Map.empty[String, DataType]
        }
      case _ => throw new IllegalArgumentException(s"Malformed JSON: \n$json")
    }
  }

}
