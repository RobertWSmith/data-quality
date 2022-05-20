package com.github.robertwsmith.dataquality.spark.ml.transform

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

/** Parameters for the auto-repartition configuration
  */
trait AutoRepartitionParams {
  self: Params =>

  /** Partition by columns parameter. The unique values of theses fiels will try to be on the same exact partition.
    *
    * @group param
    */
  final val partitionCols: StringArrayParam = new StringArrayParam(
    this,
    "groupByColumns",
    "grouping columns applied later",
    (x: Array[String]) => {
      Option(x) match {
        case Some(y) => y.toSet.size == y.length
        case _       => false
      }
    }
  )

  /** Optimal number of bytes per partition parameter. The number of partitions will target this size for each partition.
    *
    * @group param
    */
  final val optimalBytes: Param[Long] = new Param[Long](
    this,
    "optimalBytes",
    "Optimal number of bytes per in-memory partition",
    ParamValidators.gt(0L)
  )

  /** Sample row count parameter. Determines number of rows selected to estimate the optimal number of rows per partition.
    *
    * @group param
    */
  final val sampleRowCount: Param[Int] = new Param[Int](
    this,
    "sampleRowCount",
    "Sample row count taken to help estimate optimal row count",
    ParamValidators.gt(0L)
  )

  /** @group getParam */
  final def getOptimalBytes: Long = $(optimalBytes)

  /** @group getParam */
  final def getSampleRowCount: Int = $(sampleRowCount)

  /** Validate configuration against the provided schema
    *
    * @param schema [[org.apache.spark.sql.Dataset]] schema
    */
  protected def validateConfiguration(schema: StructType): Unit = {
    getPartitionCols.foreach { pc =>
      require(
        schema.fieldNames.contains(pc),
        s"Column `${pc}` not found in [${schema.fieldNames.mkString(", ")}]"
      )
    }
  }

  /** @group getParam */
  final def getPartitionCols: Array[String] = $(partitionCols)

  setDefault(
    partitionCols  -> Array.empty[String],
    optimalBytes   -> (128L * 1024L * 1024L),
    sampleRowCount -> 10000
  )

}

class AutoRepartition(override val uid: String)
    extends Estimator[AutoRepartitionModel]
    with AutoRepartitionParams
    with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("repartition"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateConfiguration(schema)
    schema
  }

  override def fit(dataset: Dataset[_]): AutoRepartitionModel = {
    validateConfiguration(dataset.schema)
    val numPartitions: Int =
      AutoRepartition.optimizeInMemoryPartitionCount(dataset, getSampleRowCount, getOptimalBytes)
    val partCnt: Int = if (numPartitions < 1) 1 else numPartitions
    new AutoRepartitionModel(uid, partCnt).copy(this, extractParamMap()).setParent(this)
  }

  /** @group setParam */
  def appendPartitionCol(value: String): this.tyoe = setPartitionCols(getPartitionCols :+ value)

  /** @group setParam */
  def setPartitionCols(values: Array[String]): this.type = set(partitionCols, values)

  /** @group setParam */
  def setOptimalBytes(value: Long): this.type = set(optimalBytes, value)

  /** @group setParam */
  def setSampleRowCount(value: Int): this.type = set(sampleRowCount, value)
}

object AutoRepartition extends DefaultParamsReadable[AutoRepartition] {

  override def load(path: String): AutoRepartition = super.load(path)

  def optimizeInMemoryPartitionCount(
    dataset: Dataset[_],
    sampleRowCount: Int = 1000,
    optimalBytes: Long = 128L * 1024L * 1024L
  ): Int = {
    val count: Long            = dataset.count()
    val rowBytes: Long         = this.estimateRowSize(dataset, sampleRowCount)
    val rowsPerPartition: Long = (optimalBytes.toDouble / rowBytes.toDouble).ceil.toLong
    val partitionCount: Int    = (count.toDouble / rowsPerPartition.toDouble).ceil.toInt
    partitionCount
  }

  def estimateRowSize(
    dataset: Dataset[_],
    sampleRowCount: Int = 1000,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): Long = {
    val rddSample: RDD[Row] = dataset.toDF().limit(sampleRowCount).rdd.persist(storageLevel)

    val totalSize: Long = rddSample
      .mapPartitions { iterator =>
        iterator.toSeq.map { r =>
          SizeEstimator.estimate(r.asInstanceOf[AnyRef])
        }.toIterator
      }
      .reduce(_ + _)

    val approxRowBytes: Long = (totalSize.toDouble / rddSample.count().toDouble).toLong

    rddSample.unpersist()
    approxRowBytes
  }

}

class AutoRepartitionModel(override val uid: String, val numPartitions: Int)
    extends Model[AutoRepartitionModel]
    with AutoRepartitionParams
    with DefaultParamsWritable {
  def this(numPartitions: Int) = this(Identifiable.randomUID("repartition"), numPartitions)

  override def copy(extra: ParamMap): AutoRepartitionModel = {
    val copied = new AutoRepartitionModel(uid, numPartitions)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateConfiguration(dataset.schema)

    if (getPartitionCols.nonEmpty)
      dataset.toDF().repartition(numPartitions, getPartitionCols.map(col): _*)
    else
      dataset.toDF().repartition(numPartitions)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateConfiguration(schema)
    schema
  }

}

object AutoRepartitionModel extends DefaultParamsReadable[AutoRepartitionModel] {
  override def load(path: String): AutoRepartitionModel = super.load(path)
}
