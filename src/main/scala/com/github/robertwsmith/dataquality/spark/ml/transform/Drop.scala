package com.github.robertwsmith.dataquality.spark.ml.transform

import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType}

trait HasDropColumns {
  self: Params =>

  /** Collection of columns to drop from a [[org.apache.spark.sql.Dataset]] parameter.
    *
    * @group param
    */
  final val dropColumns: StringArrayParam = new StringArrayParam(
    this,
    "dropColumns",
    "Columns to drop",
    (x: Array[String]) =>
      Option(x) match {
        case Some(y) =>
          if (y.nonEmpty)
            (y.toSet.size == y.length) && y.forall(p => Option(p).isInstanceOf[Some[_]])
          else
            true
        case _ => false
      }
  )

  setDefault(dropColumns, Array.empty[String])

  protected def validateConfiguration(schema: StructType): Unit = getDropColumns.foreach { dc =>
    require(
      schema.fieldNames.contains(dc),
      s"Column `$dc` not found in [${schema.fieldNames.mkString(", ")}]"
    )
  }

  /** @group getParam */
  final def getDropColumns: Array[String] = $(dropColumns)
}

class Drop(override val uid: String)
    extends Transformer
    with HasDropColumns
    with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("drop"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateConfiguration(dataset.schema)
    getDropColumns.foldLeft(dataset.toDF())((df, dc) => df.drop(dc))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateConfiguration(schema)
    val fields: Array[StructField] = schema.fields.collect {
      case x if !getDropColumns.contains(x.name) => x
    }
    fields.foldLeft(new StructType())((st, f) => st.add(f))
  }

  /** @group setParam */
  def appendDropColumn(value: String): this.type = setDropColumns(getDropColumns :+ value)

  /** @group setParam */
  def setDropColumns(values: Array[String]): this.type = set(dropColumns, values)
}

object Drop extends DefaultParamsReadable[Drop] {
  override def load(path: String): Drop = super.load(path)
}
