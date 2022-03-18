package com.github.robertwsmith.dataquality.spark.ml.params.shared

import com.github.robertwsmith.dataquality.utilities.ThreadUtilities
import org.apache.spark.ml.param.{IntParam, Params, ParamValidators}

import scala.concurrent.ExecutionContext

trait HasParallelism extends Params {

  /** The number of threads to use when running parallel algorithms.
    * Default is 1 for serial execution.
    *
    * @group expertParam
    */
  val parallelism: IntParam = new IntParam(
    this,
    "parallelism",
    "the number of threads to use when running parallel algorithms",
    ParamValidators.gtEq(1)
  )

  setDefault(parallelism -> 1)

  /** Create a new execution context with a thread pool that has a maximum number of threads
    * set to the value of [[parallelism]]. If this param is set to 1, a same-thread executor
    * will be used to run in serial.
    */
  protected def getExecutionContext: ExecutionContext =
    getParallelism match {
      case 1 => ThreadUtilities.sameThread
      case n =>
        ExecutionContext.fromExecutorService(
          ThreadUtilities
            .newDaemonCachedThreadPool(s"${this.getClass.getSimpleName}-thread-pool", n)
        )
    }

  /** @group expertGetParam */
  def getParallelism: Int = $(parallelism)
}
