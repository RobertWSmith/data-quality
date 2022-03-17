package com.github.robertwsmith.dataquality.spark.ml.params

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.DataType
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class DataTypeParam(parent: String, name: String, doc: String, isValid: DataType => Boolean)
    extends Param[DataType](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: DataType => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (_: DataType) => true)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent, name, doc, (_: DataType) => true)

  override def jsonEncode(value: DataType): String =
    compact(render(JString(value.typeName)))

  override def jsonDecode(json: String): DataType =
    parse(json) match {
      case JString(s) => DataType.fromJson(compact(render(JString(s))))
      case _          => throw new IllegalArgumentException("Non-strings not supported")
    }

}
