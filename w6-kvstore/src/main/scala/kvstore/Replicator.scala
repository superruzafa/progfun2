package kvstore

import akka.actor.{ReceiveTimeout, Props, Actor, ActorRef}
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  val RECEIVE_TIMEOUT = 100 millisecond

  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, value, id) =>
      val msg = Snapshot(key, value, nextSeq)
      replica ! msg
      context.setReceiveTimeout(Replicator.RECEIVE_TIMEOUT)
      context.become(receivePendingSnapshotAcknowledgements(sender, id) orElse resendOnTimeout(msg))
    case ReceiveTimeout =>
      context.setReceiveTimeout(Duration.Undefined)
  }

  def receivePendingSnapshotAcknowledgements(originalSender: ActorRef, replicationId: Long): Receive = {
    case SnapshotAck(key, seq) =>
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      originalSender ! Replicated(key, replicationId)
  }

  def resendOnTimeout(messageToResend: Any): Receive = {
    case ReceiveTimeout =>
      replica ! messageToResend
  }
}
