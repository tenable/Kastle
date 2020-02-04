package com.tenable.library.kafkaclient.utils

import cats.effect.Resource
import scala.concurrent.ExecutionContext
import cats.effect.Sync
import java.util.concurrent.ThreadFactory
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

object ExecutionContexts {
  //I would love to use some of the EC utils available in other places. But none of them are a dependency to this library.
  //TODO change or remove this as soon as possible, taken from Monix

  private val defaultUncaughtExceptionHandler: (Thread, Throwable) => Unit = { (t, e) =>
    t.getUncaughtExceptionHandler match {
      case null  => e.printStackTrace()
      case other => other.uncaughtException(t, e)
    }
  }

  private def executorServiceResource[F[_]: Sync](
      name: String,
      daemonic: Boolean,
      onError: (Thread, Throwable) => Unit = defaultUncaughtExceptionHandler
  )(f: ThreadFactory => ExecutorService): Resource[F, ExecutorService] = {
    val tp = new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setDaemon(daemonic)
        t.setUncaughtExceptionHandler((t: Thread, e: Throwable) => {
          onError(t, e)
        })
        t.setName(s"$name-${t.getId}")
        t
      }
    }

    Resource.make(Sync[F].delay(f(tp)))(es => Sync[F].delay(es.shutdown()))
  }

  def io[F[_]: Sync](name: String): Resource[F, ExecutionContext] = {
    executorServiceResource(name, daemonic = true)(Executors.newCachedThreadPool)
      .map(ExecutionContext.fromExecutor(_))
  }

  def bounded[F[_]: Sync](
      name: String,
      threadsN: Int,
      daemonic: Boolean
  ): Resource[F, ExecutionContext] = {
    executorServiceResource(name, daemonic)(Executors.newFixedThreadPool(threadsN, _))
      .map(ExecutionContext.fromExecutor(_))
  }
}
