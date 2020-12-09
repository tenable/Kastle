package com.tenable.library.kafkaclient.client.standard.consumer

import cats.instances.try_._
import cats.instances.either._
import cats.syntax.foldable._
import cats.syntax.applicativeError._
import cats.{ ApplicativeError, Foldable, Show }
import com.tenable.library.kafkaclient.client.standard.consumer.actions.ProcessAction

import scala.util.Try

trait EventActionable[A] {
  def asProcessAction(processResult: A): ProcessAction
}

object EventActionable {
  def apply[A](implicit EA: EventActionable[A]): EventActionable[A] = EA

  implicit def deriveFromApplicativeError[G[_]: Foldable, E: Show](
    implicit AE: ApplicativeError[G, E]
  ): EventActionable[G[Unit]] =
    (processResult: G[Unit]) => {
      processResult
        .attempt
        .foldLeft[ProcessAction](ProcessAction.rejectAll("Unknown Error")) {
          case (_, Left(e))  => ProcessAction.rejectAll(Show[E].show(e))
          case (_, Right(_)) => ProcessAction.commitAll
        }
    }

  implicit def deriveFromEither[E](implicit S: Show[E]): EventActionable[Either[E, Unit]] =
    deriveFromApplicativeError[Either[E, *], E]

  implicit def deriveFromTry(implicit S: Show[Throwable]): EventActionable[Try[Unit]] =
    deriveFromApplicativeError[Try[*], Throwable]

  def deriveFromResult[A](f: A => ProcessAction): EventActionable[A] =
    (a: A) => f(a)

  implicit val deriveFromProcessAction: EventActionable[ProcessAction] =
    deriveFromResult(identity)
}
