package com.github.robertwsmith.dataquality.spark.ml.transform

import com.github.robertwsmith.dataquality.spark.ml.params.StringDataTypeMapParam
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructField, StructType}

trait HasCasts {
  self: Params =>

  /** Cast columns to a given [[org.apache.spark.sql.types.DataType]] parameter.
    *
    * @group param
    */
  final val casts: StringDataTypeMapParam = new StringDataTypeMapParam(
    this,
    "casts",
    "Column name and DataType pairs",
    (x: Map[String, DataType]) =>
      Option(x) match {
        case Some(_) => true
        case _       => false
      }
  )

  /** Validate configuration against the provided schema
    *
    * @param schema [[org.apache.spark.sql.Dataset]] schema
    */
  def validateConfiguration(schema: StructType): Unit =
    getCasts.keys.foreach { cc =>
      require(
        schema.fieldNames.contains(cc),
        s"Column `${cc}` not found in [${schema.fieldNames.mkString(", ")}]"
      )
    }

  /** @group getParam */
  final def getCasts: Map[String, DataType] = $(casts)
}

class Cast(override val uid: String) extends Transformer with HasCasts with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("cast"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateConfiguration(dataset.schema)
    getCasts.foldLeft(dataset.toDF()) { case (df, (name, dataType)) =>
      df.withColumn(name, col(name).cast(dataType))
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateConfiguration(schema)

    schema.fields.foldLeft(new StructType())((st, f) => {
      val field: StructField =
        if (getCasts.contains(f.name))
          f.copy(dataType = getCasts(f.name))
        else
          f

      st.add(field)
    })
  }

  /** @group setParam */
  def appendCast(fieldName: String, dataType: DataType): this.type = {
    val tempCasts: Map[String, DataType] = getCasts + (fieldName -> dataType)
    setCasts(tempCasts)
  }

  /** @group setParam */
  def setCasts(newCasts: Map[String, DataType]): this.type = set(casts, newCasts)

  setDefault(casts -> Map.empty[String, DataType])
}

object Cast extends DefaultParamsReadable[Cast] {
  override def load(path: String): Cast = super.load(path)
}
