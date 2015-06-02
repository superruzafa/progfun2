package kvstore

import akka.actor.{ActorRef, Actor}
import kvstore.Replicator.{Replicated, Replicate}
import scala.concurrent.duration._
import scala.language.postfixOps
import kvstore.GlobalReplicator.{ReplicatorsRemoved, GlobalReplicationSuccess, GlobalReplicationTimedOut}
import kvstore.Persistence.Persist
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

object GlobalReplicator {
  case class GlobalReplicationTimedOut(replicate: Replicate)
  case class GlobalReplicationSuccess(replicate: Replicate)
  case class ReplicatorsRemoved(replicators: Set[ActorRef])
}

class GlobalReplicator(val replicate: Replicate,
                       val replicators: Set[ActorRef],
                       val timeoutPeriod: FiniteDuration = 1 second) extends Actor {

  if (replicators.isEmpty) {
    onSuccess()
  } else {
    context.become(receiveReplicated(replicators))
    context.system.scheduler.scheduleOnce(timeoutPeriod) {
      self ! GlobalReplicationTimedOut(replicate)
    }
    replicators.foreach(rep => rep ! replicate)
  }

  def receive: Receive = {
    case _ =>
  }

  def receiveReplicated(remainingReplicators: Set[ActorRef]): Receive = {
    case Replicated(_, _) =>
      handleRemainingReplicators(remainingReplicators - sender)
    case ReplicatorsRemoved(replis) =>
      handleRemainingReplicators(remainingReplicators -- replis)
    case t@GlobalReplicationTimedOut(r) =>
      context.stop(self)
      context.parent ! t
  }

  private def handleRemainingReplicators(newRemainingReplicators: Set[ActorRef]) = {
    if (newRemainingReplicators.isEmpty) {
      onSuccess()
    } else {
      context.become(receiveReplicated(newRemainingReplicators))
    }
  }

  def onSuccess() {
    context.stop(self)
    context.parent ! GlobalReplicationSuccess(replicate)
  }
}
