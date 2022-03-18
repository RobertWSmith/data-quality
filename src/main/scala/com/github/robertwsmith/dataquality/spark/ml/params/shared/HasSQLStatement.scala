package com.github.robertwsmith.dataquality.spark.ml.params.shared

import org.apache.spark.ml.param.{Param, Params}

trait HasSQLStatement {
  self: Params =>

  /** SQL statement parameter. A SQL statement which is provided in string form.
    *
    * @group param
    */
  final val statement: Param[String] = new Param[String](
    this,
    "statement",
    "SQL column statement",
    (x: String) =>
      Option(x) match {
        case Some(y) => y.nonEmpty
        case _       => false
      }
  )

  setDefault(statement -> "NULL")

  /** @group getParam */
  def getStatement: String = $(statement)
}
