package com.github.robertwsmith.dataquality.utilities

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.types._

import scala.annotation.tailrec

object Spark {

  /** Unpivot a [[Dataset[_] ]] from wide to tall format.
    *
    * @param dataset source dataset in wide format
    * @param idVars columns used as identifier variables
    * @param valueVars columns to unpivot, if empy uses all columns not set as `id_vars`
    * @param varName name to use for the `variable` column, if missing uses `variable`
    * @param valueName name to use dor the `value` column
    * @param valueDataType data type to use to unify the various values
    * @param tempNameRoot temp name to use for variable creation
    * @return new [[DataFrame]] in tall/normalized form
    */
  def melt(
    dataset: Dataset[_],
    idVars: Seq[String],
    valueVars: Seq[String],
    varName: String,
    valueName: String,
    valueDataType: DataType,
    tempNameRoot: String = "melt_temp"
  ): DataFrame = {
    val requiredColumns: Seq[String] = idVars ++ valueVars
    try {
      validateMeltInputs(
        dataset,
        idVars,
        valueVars,
        varName,
        valueName,
        valueDataType,
        requiredColumns
      )
    } catch {
      case i: IllegalArgumentException => println(s"Melt input validation failed: $i"); throw i
    }

    val tempName: String = this.makeTempName(dataset, tempNameRoot)

    val arrayOfStructures: Column = {
      val structures: Seq[Column] = valueVars.map { v =>
        struct(
          lit(v.toLowerCase()).cast(StringType).alias(varName),
          col(v).cast(valueDataType).alias(valueName)
        )
      }
      array(structures: _*)
    }

    val finalSelect: Seq[Column] = idVars.map(col) ++ Seq(
      col(tempName)(varName).alias(varName),
      col(tempName)(valueName).alias(valueName)
    )

    dataset.toDF().withColumn(tempName, explode(arrayOfStructures)).select(finalSelect: _*)
  }

  @throws[IllegalArgumentException]
  private def validateMeltInputs(
    dataset: Dataset[_],
    idVars: Seq[String],
    valueVariables: Seq[String],
    varName: String,
    valueName: String,
    valueDataType: DataType,
    requiredColumns: Seq[String]
  ): Unit = {
    (idVars ++ valueVariables).foreach { i: String =>
      require(
        dataset.columns.contains(i),
        s"Dataset does not contain a field name `${i.trim}` out of: [${dataset.columns.mkString(", ")}]"
      )
    }

    require(valueDataType match {
      case DoubleType | FloatType          => true
      case dt: DecimalType if dt.scale > 0 => true
      case _                               => false
    })

    require(
      requiredColumns.distinct.size == requiredColumns.size,
      "Duplicate column names in required columns"
    )
  }

  @tailrec
  private def makeTempName(dataset: Dataset[_], tempNameRoot: String, counter: Int = 0): String = {
    if (!dataset.columns.contains(tempNameRoot))
      tempNameRoot
    else if (
      dataset.columns.contains(tempNameRoot)
        & !dataset.columns.contains(s"${tempNameRoot}_$counter")
    )
      s"${tempNameRoot}_$counter"
    else
      makeTempName(dataset, tempNameRoot, counter + 1)
  }

}
