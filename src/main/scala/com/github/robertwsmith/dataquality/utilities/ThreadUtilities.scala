package com.github.robertwsmith.dataquality.utilities

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

import java.util.concurrent._
import scala.concurrent.{
  ExecutionContext,
  ExecutionContextExecutor,
  ExecutionContextExecutorService
}

object ThreadUtilities {

  private lazy val sameThreadExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(MoreExecutors.sameThreadExecutor())

  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor =
    Executors
      .newCachedThreadPool(namedThreadFactory(prefix))
      .asInstanceOf[ThreadPoolExecutor]

  def namedThreadFactory(prefix: String): ThreadFactory =
    new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(prefix + "-%d")
      .build()

  def newDaemonCachedThreadPool(
    prefix: String,
    maxThreadNumber: Int,
    keepAliveSeconds: Int = 60
  ): ThreadPoolExecutor = {
    val threadPool: ThreadPoolExecutor = new ThreadPoolExecutor(
      maxThreadNumber,
      maxThreadNumber,
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      namedThreadFactory(prefix)
    )
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

}
