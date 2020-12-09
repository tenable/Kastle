package com.tenable.library.kafkaclient.client.standard.consumer.actions

import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.semigroup._
import cats.instances.list._
import cats.{ Monad, Monoid }
import com.tenable.library.kafkaclient.client.standard.KafkaConsumerIO
import com.tenable.library.kafkaclient.client.standard.consumer.{ BatchContext, GOffsets, PausedTemporarily }
import org.apache.kafka.common.TopicPartition

sealed trait ProcessAction
object ProcessAction {

  sealed trait KafkaAction
  case object DoNothing                                                             extends KafkaAction
  case class Commit(except: Option[Set[TopicPartition]] = None)                     extends KafkaAction
  case class Reject(error: Option[String], pauseDetails: Option[PausedTemporarily]) extends KafkaAction

  // If no topic partition defined action will be executed on all topic partitions offsets
  case class SingleAction(kafkaAction: KafkaAction, maybeTP: Option[TopicPartition]) extends ProcessAction
  case class MultiAction(actions: List[SingleAction])                                extends ProcessAction
  object MultiAction {
    val empty: MultiAction = MultiAction(List.empty)
  }

  def single(kafkaAction: KafkaAction, tp: TopicPartition): ProcessAction =
    SingleAction(kafkaAction, Some(tp))

  def all(kafkaAction: KafkaAction): ProcessAction =
    SingleAction(kafkaAction, None)

  def doNothing: ProcessAction = all(DoNothing)
  def commitAll: ProcessAction =
    all(Commit())

  def commitAllExcept(tps: Set[TopicPartition]): ProcessAction =
    all(Commit(except = Some(tps)))

  def rejectAll(error: String): ProcessAction =
    all(Reject(Some(error), None))

  def rejectPauseAll(details: PausedTemporarily, error: String): ProcessAction =
    all(Reject(Some(error), Some(details)))

  def commitSome(tps: Set[TopicPartition]): ProcessAction =
    MultiAction(tps.map(tp => SingleAction(Commit(), Some(tp))).toList)

  def rejectSome(tps: Set[TopicPartition], error: Option[String]): ProcessAction =
    MultiAction(tps.map(tp => SingleAction(Reject(error, None), Some(tp))).toList)

  def rejectPauseSome(
    tps: Set[TopicPartition],
    details: PausedTemporarily,
    error: Option[String]
  ): ProcessAction =
    MultiAction(tps.map(tp => SingleAction(Reject(error, Some(details)), Some(tp))).toList)

  def rejectSomeCommitRest(
    tps: Set[TopicPartition],
    error: String,
    maybePauseDetails: Option[PausedTemporarily]
  ): ProcessAction =
    maybePauseDetails match {
      case Some(pd) => rejectPauseSome(tps, pd, Some(error)) |+| commitAllExcept(tps)
      case None     => rejectSome(tps, Some(error)) |+| commitAllExcept(tps)
    }

  def rejectSomeCommitSome(
    commitTps: Set[TopicPartition],
    rejectTps: Set[TopicPartition],
    error: String,
    maybePauseDetails: Option[PausedTemporarily]
  ): ProcessAction = maybePauseDetails match {
    case Some(pd) => rejectPauseSome(rejectTps, pd, Some(error)) |+| commitSome(commitTps)
    case None     => rejectSome(rejectTps, Some(error)) |+| commitSome(commitTps)
  }

  private[consumer] def interpret[F[_]: Monad](
    kafkaIO: KafkaConsumerIO[F, _, _],
    action: ProcessAction,
    offsets: Map[TopicPartition, GOffsets]
  ): F[BatchContext] =
    action match {
      case MultiAction(actions) =>
        actions.foldLeftM(BatchContext.empty) { (acc, action) =>
          doInterpret[F](kafkaIO, action, offsets).map(acc |+| _)
        }
      case s @ SingleAction(_, _) => doInterpret[F](kafkaIO, s, offsets)
    }

  // scalastyle:off cyclomatic.complexity
  private def doInterpret[F[_]: Monad](
    kIO: KafkaConsumerIO[F, _, _],
    action: SingleAction,
    offsets: Map[TopicPartition, GOffsets]
  ): F[BatchContext] = {

    def using[R](tp: TopicPartition)(fn: GOffsets => R): Option[R] =
      offsets.get(tp).map(fn)

    def commitUsing(tp: TopicPartition): Map[TopicPartition, Long] =
      using(tp)(_.commit).map(tp -> _).toMap

    def rejectUsing(tp: TopicPartition): Map[TopicPartition, Long] =
      using(tp)(_.reject).map(tp -> _).toMap

    action match {
      // commit all except X
      case SingleAction(DoNothing, _) => Monad[F].pure(BatchContext.empty)
      case SingleAction(Commit(Some(except)), None) =>
        kIO
          .commitSync(
            offsets.filterNot { case (tp, _) => except.contains(tp) }.view.mapValues(_.commit).toMap
          )
          .as(BatchContext.empty)
      case SingleAction(Commit(Some(except)), Some(tp)) =>
        kIO
          .commitSync(commitUsing(tp).filterNot { case (ttp, _) => except.contains(ttp) })
          .as(BatchContext.empty)

      case SingleAction(Commit(None), None) =>
        kIO.commitSync(offsets.view.mapValues(_.commit).toMap).as(BatchContext.empty)
      case SingleAction(Commit(None), Some(tp)) =>
        kIO.commitSync(commitUsing(tp)).as(BatchContext.empty)

      case SingleAction(Reject(error, None), None) =>
        val offsetsR = offsets.view.mapValues(_.reject).toMap
        kIO.seekWithError(offsetsR, error).as(BatchContext(offsetsR.keySet))
      case SingleAction(Reject(error, None), Some(tp)) =>
        val offsetsR = rejectUsing(tp)
        kIO.seekWithError(offsetsR, error).as(BatchContext(offsetsR.keySet))

      case SingleAction(Reject(error, Some(pauseDetails)), None) =>
        val offsetsR = offsets.view.mapValues(_.reject).toMap
        for {
          _ <- kIO.seekWithError(offsetsR, error)
          _ <- kIO.pause(offsetsR.view.mapValues(_ => pauseDetails).toMap)
        } yield BatchContext(offsetsR.keySet)
      case SingleAction(Reject(error, Some(pauseDetails)), Some(tp)) =>
        val offsetsR = rejectUsing(tp)
        for {
          _ <- kIO.seekWithError(offsetsR, error)
          _ <- kIO.pause(offsetsR.view.mapValues(_ => pauseDetails).toMap)
        } yield BatchContext(offsetsR.keySet)
    }
  }
  // scalastyle:on cyclomatic.complexity

  implicit val postProcessActionM: Monoid[ProcessAction] =
    new Monoid[ProcessAction] {
      override def empty: ProcessAction =
        doNothing

      override def combine(left: ProcessAction, right: ProcessAction): ProcessAction =
        (left, right) match {
          case (SingleAction(DoNothing, _), ar @ SingleAction(_, _)) => ar
          case (al @ SingleAction(_, _), SingleAction(DoNothing, _)) => al
          case (al @ SingleAction(_, _), ar @ SingleAction(_, _))    => MultiAction(List(al, ar))
          case (al: MultiAction, SingleAction(DoNothing, _))         => al
          case (MultiAction(actions), ar @ SingleAction(_, _))       => MultiAction(actions :+ ar)
          case (SingleAction(DoNothing, _), ar: MultiAction)         => ar
          case (al @ SingleAction(_, _), MultiAction(actions))       => MultiAction(al +: actions)
          case (MultiAction(la), MultiAction(ra))                    => MultiAction(la ++ ra)
        }
    }
}
