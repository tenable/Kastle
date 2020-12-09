package com.tenable.library.kafkaclient.client.standard.consumer

import java.time.Instant

import cats.effect.concurrent.MVar2
import cats.effect.{CancelToken, ConcurrentEffect, ContextShift, Timer}
import cats.syntax.applicativeError._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.tenable.library.kafkaclient.utils.Converters.JavaDurationOps
import org.apache.kafka.clients.consumer.{Consumer, CommitFailedException}
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

private[standard] class ConsumerStateHandler[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    val clientId: String,
    keepAliveInterval: FiniteDuration,
    builder: () => Consumer[K, V],
    mVarConsumer: MVar2[F, State[F, K, V]],
    blockingEC: ExecutionContext
)(implicit logger: Logger) {
  private val F  = ConcurrentEffect[F]
  private val CS = ContextShift[F]
  private val T  = Timer[F]

  def withConsumer[R](action: String, debugMsg: Option[String] = None)(
      fConsumer: State[F, K, V] => F[(State[F, K, V], R)]
  ): F[R] =
    CS.evalOn(blockingEC) {
      mVarConsumer.take.flatMap { c =>
        debugMsg.fold(F.unit)(m => F.delay(logger.debug(s"Action: $action... $m"))) *>
          fConsumer(c)
            .flatMap(res => mVarConsumer.put(res._1).as(res._2))
            .onError {
              case e => {
                def logThrowableByLevel(msg: String, e: Throwable): Unit =
                  e match {
                    case _: CommitFailedException => logger.warn(msg, e)
                    case _                        => logger.error(msg, e)
                  }
                F.delay(logThrowableByLevel(s"Failed $action with error ${e.getMessage}", e)) *>
                  mVarConsumer.put(c)
              }
            }
      }
    }

  def unsafeStart(pausedTopics: Set[String]): F[Unit] =
    for {
      _ <- F.delay(logger.info(s"Starting consumer $clientId"))
      newC = {
        val newC = builder()
        val tps =
          newC.assignment().asScala.filter(tp => pausedTopics.exists(t => t == tp.topic()))
        newC.pause(tps.asJava)
        newC
      }
      cancelKA <- runKeepAlive(keepAliveInterval)
      _        <- mVarConsumer.put(State(newC, Map.empty, pausedTopics, cancelKA))
      _        <- F.delay(logger.info(s"Started new consumer $clientId"))
    } yield ()

  def restartOnError(error: Throwable): F[Unit] = error match {
    case NonFatal(t) =>
      F.delay(logger.warn("Unhandled error, restarting kafka", t)) *> closeConsumer(
        timeout = None
      ) *> {
        for {
          state <- mVarConsumer.take
          _     <- F.delay(logger.info(s"Restarting consumer $clientId"))
          _     <- unsafeStart(state.pausedT)
          _     <- F.delay(logger.info(s"Restarted new consumer $clientId"))
        } yield ()
      }
    case _ => F.raiseError(error)
  }

  def closeConsumer(timeout: Option[Duration]): F[Unit] =
    withConsumer("close") { state =>
      val closeConsumer = F
        .delay(timeout.map(_.asJavaDuration).fold(state.consumer.close())(state.consumer.close))
        .as((state.copy(isClosed = true), ()))

      F.delay(logger.info(s"Closing consumer $clientId")) *>
        state.keepAliveCancel *>
        closeConsumer <*
        F.delay(logger.info(s"Closed consumer $clientId"))
    }

  private def runKeepAlive(interval: FiniteDuration): F[CancelToken[F]] = {
    if (interval.toMillis == 0) { noKeepAlive }
    else { withKeepAlive(interval) }
  }

  private def noKeepAlive: F[CancelToken[F]] = {
    F.delay(F.unit)
  }

  private def withKeepAlive(interval: FiniteDuration): F[CancelToken[F]] = {
    def keepAlive(): F[Unit] =
      withConsumer("keep-alive") { state =>
        F.delay {
          val paused = state.consumer.paused()
          state.consumer.pause(state.consumer.assignment())
          val r = state.consumer.poll(java.time.Duration.ofMillis(1))
          if (!r.isEmpty) {
            F.raiseError(
              new IllegalStateException("Unexpectedly received records in a keep alive polling")
            )
          } else {
            state.consumer.resume(state.consumer.assignment())
          }
          state.consumer.pause(paused)
          (state, ())
        }
      }

    val keepAliveF =
      F.delay(logger.info(s"Running keep-alive check every ${interval.toSeconds} seconds")) *>
        keepAlive().recoverWith { case NonFatal(t) =>
          F.delay(logger.error("Unhandled error while doing keep alives", t)) *> F.unit
        } <* T.sleep(interval)

    F.start {
      F.cancelable[Unit] { _ =>
        val fiberF = F.start(keepAliveF.foreverM[Unit])
        val fiber  = F.toIO(fiberF).unsafeRunSync()

        for {
          _ <- F.delay(logger.info("Cancelling keep-alive"))
          _ <- fiber.cancel
          _ <- F.delay(logger.info("Cancelled keep-alive"))
        } yield ()
      }
    }.map(_.cancel)
  }

  def refreshTemporarilyPaused(): F[Unit] =
    withConsumer("refresh-temp-paused") { state =>
      val toAwaken = findPartitionsToAwaken(state)
      val filteredPermPaused =
        toAwaken.filterNot(t => state.pausedT.exists(tn => tn == t.topic()))

      if (filteredPermPaused.isEmpty) {
        F.pure((state, ()))
      } else {
        F.delay {
          logger.info(s"Resuming due for topic $filteredPermPaused")
          state.consumer.resume(filteredPermPaused.asJava)
          (state.copy(pausedTP = state.pausedTP -- filteredPermPaused), ())
        }
      }
    }

  private def findPartitionsToAwaken(state: State[F, K, V]): Set[TopicPartition] =
    state.pausedTP.collect {
      case (tp, details)
          if details.when.plusMillis(details.duration.toMillis).isBefore(Instant.now) =>
        tp
    }.toSet
}

case class PausedTemporarily(when: Instant, duration: FiniteDuration)
case class State[F[_], K, V](
    consumer: Consumer[K, V],
    pausedTP: Map[TopicPartition, PausedTemporarily],
    pausedT: Set[String],
    keepAliveCancel: CancelToken[F],
    isClosed: Boolean = false
)
