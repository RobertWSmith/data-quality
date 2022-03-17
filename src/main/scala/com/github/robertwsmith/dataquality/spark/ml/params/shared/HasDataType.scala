package com.github.robertwsmith.dataquality.spark.ml.params.shared

import com.github.robertwsmith.dataquality.spark.ml.params.DataTypeParam
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.types.{DataType, DataTypes}

trait HasDataType {

  self: Params =>

  /** Data Type parameter. Defines a [[DataType]] which is used by a [[org.apache.spark.ml.PipelineStage]] instance.
    *
    * @group param
    */
  final val dataType: DataTypeParam = new DataTypeParam(
    this,
    "dataType",
    "Field DataType",
    (x: DataType) =>
      Option(x) match {
        case Some(_) => true
        case None    => false
      }
  )

  setDefault(dataType -> DataTypes.NullType)

  final def getDataType: DataType = $(dataType)

}
