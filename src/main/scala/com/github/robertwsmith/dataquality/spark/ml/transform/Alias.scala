package com.github.robertwsmith.dataquality.spark.ml.transform

import com.github.robertwsmith.dataquality.spark.ml.params.StringMapParam
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType}

/** Defines a [[org.apache.spark.ml.PipelineStage]] which renames columns
  */
trait HasAliases {
  self: Params =>

  /** Pairs of aliases to apply to the input [[Dataset]]
    *
    * @group param
    */
  final val aliases: StringMapParam = new StringMapParam(
    this,
    "aliases",
    "Old name to new name pairs",
    (x: Map[String, String]) => {
      def testNotNull[T](value: T): Boolean =
        Option(value) match {
          case Some(_) => true
          case _       => false
        }

      if (testNotNull(x)) {
        x.forall(p =>
          if (testNotNull(p._1) && testNotNull(p._2))
            p._1 != p._2
          else
            false
        )
      } else
        false
    }
  )

  setDefault(aliases -> Map.empty[String, String])

  /** Validate configuration against the provided schema.
    *
    * @param schema [[Dataset]] schema
    */
  def validateConfiguration(schema: StructType): Unit = {
    getAliases.keys.foreach { ac =>
      require(
        schema.fieldNames.contains(ac),
        s"Column `${ac}` not found in [${schema.fieldNames.mkString(", ")}]"
      )
    }
  }

  /** @group getParam */
  def getAliases: Map[String, String] = $(aliases)
}

class Alias(override val uid: String)
    extends Transformer
    with HasAliases
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("alias"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateConfiguration(dataset.schema)

    getAliases.foldLeft(dataset.toDF()) { case (df, (oldName, newName)) =>
      df.withColumnRenamed(oldName, newName)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateConfiguration(schema)

    schema.fields.foldLeft(new StructType())((st, f) => {
      val field: StructField =
        if (getAliases.contains(f.name))
          f.copy(name = getAliases(f.name))
        else
          f

      st.add(field)
    })
  }

  /** @group setParam */
  def appendAlias(oldName: String, newName: String): this.type = {
    val tempAliases: Map[String, String] = getAliases + (oldName -> newName)
    setAliases(tempAliases)
  }

  /** @group setParam */
  def setAliases(newAliases: Map[String, String]): this.type = set(aliases, newAliases)
}

object Alias extends DefaultParamsReadable[Alias] {

  override def load(path: String): Alias = super.load(path)

}
