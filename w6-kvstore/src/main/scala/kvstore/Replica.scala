package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.concurrent.duration._
import scala.language.postfixOps
import kvstore.Replica._
import kvstore.Replica.Remove
import kvstore.Persistence.Persist
import kvstore.Replicator.{Replicate, Snapshot, SnapshotAck}
import kvstore.Replica.Get
import kvstore.Replica.GetResult
import kvstore.Replica.Insert

object Replica {
  val PersistDuration: Duration = 1 second

  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(subContracted(context.actorOf(Props(new PrimaryReplica(persistenceProps)))))
    case JoinedSecondary => context.become(subContracted(context.actorOf(Props(new SecondaryReplica(persistenceProps)))))
  }

  def subContracted(subContractor: ActorRef): Receive = {
    case m => subContractor forward m
  }
}