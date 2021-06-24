package com.tenable.library.kafkaclient.utils

import java.time.{ Duration => JDuration }
import java.util.concurrent.{ Future => JFuture }

import cats.syntax.functor._
import cats.syntax.applicativeError._

import scala.concurrent.duration.Duration
import cats.effect.Async

object Converters {

  implicit class JavaFutureOps[R](val self: JFuture[R]) extends AnyVal {
    def liftJF[F[_]: Async]: F[R] = liftJavaFuture[F, R](self)
  }

  def liftJavaFuture[F[_]: Async, R](future: JFuture[R]): F[R] = {
    val F = Async[F]
    F.async_[R] { cb =>
      F.delay(future.get)
        .map(r => cb(Right(r)))
        .recover { case e => cb(Left(e)) }
      ()
    }
  }

  implicit class JavaDurationOps(val self: Duration) extends AnyVal {
    def asJavaDuration: JDuration = convertToJavaDuration(self)
  }

  def convertToJavaDuration(duration: Duration): JDuration =
    JDuration.ofMillis(duration.toMillis)
}
