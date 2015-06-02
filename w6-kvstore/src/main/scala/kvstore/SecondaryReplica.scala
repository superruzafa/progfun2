package kvstore

import akka.actor.{ActorRef, Actor, Props}
import kvstore.Replicator.{Snapshot, SnapshotAck}
import kvstore.Persistence.Persist
import kvstore.Replica.{Get, GetResult}
import kvstore.Persister.{PersistFailed, PersistComplete}

class SecondaryReplica(persistenceProps: Props) extends Actor {
  val persistenceActor = context.actorOf(persistenceProps)

  var _expectedSeqCounter = 0L
  def nextExpectedSeq = {
    val value = _expectedSeqCounter
    _expectedSeqCounter += 1
    value
  }

  context.become(receive(expectedSeq = nextExpectedSeq))

  def receive: Receive = {
    case _ =>
  }

  def receive(kv: Map[String, String] = Map.empty[String, String],
              pendingPersists: Map[Long, ActorRef] = Map.empty[Long, ActorRef],
              expectedSeq: Long): Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case PersistComplete(key, id) if pendingPersists.contains(id) =>
      val client = pendingPersists.get(id).get
      client ! SnapshotAck(key, id)
      context.become(receive(kv, pendingPersists - id, expectedSeq))

    case PersistFailed(key, id) if pendingPersists.contains(id) =>
      context.become(receive(kv, pendingPersists - id, expectedSeq))

    case Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      val newKv =
        if (valueOption.nonEmpty)
          kv + (key -> valueOption.get)
        else
          kv - key

      val persist = Persist(key, valueOption, seq)
      context.actorOf(Props(new Persister(persist, persistenceActor)), "Secondary_Persister" + seq)
      context.become(receive(newKv, pendingPersists + (seq -> sender), nextExpectedSeq))

    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)

    case _ =>
  }
}
