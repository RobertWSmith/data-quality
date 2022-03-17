package com.github.robertwsmith.dataquality.spark.ml.aggregate

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

abstract class AggregateRoot(val name: String) {

  /** Display name for the aggregation
    *
    * @return default to `name`, otherwise defined by implementation
    */
  def configuredName: String = this.name
}

object AggregateRoot {

  /** Default sorting order
    */
  implicit val ordering: Ordering[AggregateRoot] = new Ordering[AggregateRoot] {
    override def compare(x: AggregateRoot, y: AggregateRoot): Int =
      x.name.compareTo(y.name)
  }

}

abstract class UnivariateAggregate(name: String) extends AggregateRoot(name) {

  /** Generate aggregate column expression
    *
    * @param column source column
    * @return aggregate expression column
    */
  def apply(column: Column): Column

  /** Generate aggregate column expression from source column name
    *
    * @param column source column name
    * @return aggregate expression column
    */
  def apply(column: String): Column =
    this.apply(col(column))
}

/** Base type representing a bivariate aggregate summary.
  *
  * @param name detailed name for the type of aggregation
  */
abstract class BivariateAggregate(name: String) extends AggregateRoot(name) {

  /** Generate aggregate column expression from two columns
    *
    * @param columnOne The first or left hand side column
    * @param columnTwo The second or right hand side column
    * @return aggregate expression column
    */
  def apply(columnOne: Column, columnTwo: Column): Column

  /** Generate aggregate column expression from two column names
    *
    * @param columnOne The first or left hand side column name
    * @param columnTwo The second or right hand side column name
    * @return aggregate expression column
    */
  def apply(columnOne: String, columnTwo: String): Column =
    this.apply(col(columnOne), col(columnTwo))

}

abstract class MultivariateAggregate(name: String) extends AggregateRoot(name) {

  /** Generate aggregate column expression from two or more columns
    *
    * @param columnOne The first or left hand side column
    * @param columnTwo The second or right hand side column
    * @param columnN The third through Nth columns
    * @return aggregate expression column
    */
  def apply(columnOne: Column, columnTwo: Column, columnN: Column*): Column

  /** Generate aggregate column expression from two or more column names
    *
    * @param columnOne The first or left hand side column name
    * @param columnTwo The second or right hand side column name
    * @param columnN The third through Nth column names
    * @return aggregate expression column
    */
  def apply(columnOne: String, columnTwo: String, columnN: String*): Column =
    this.apply(col(columnOne), col(columnTwo), columnN.map(col): _*)

}
