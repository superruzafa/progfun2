package kvstore

import akka.actor.{ActorRef, Actor, Props}
import kvstore.Replica._
import kvstore.Replica.OperationFailed
import kvstore.Replica.Remove
import kvstore.Replicator.Replicate
import kvstore.Persistence.Persist
import scala.Some
import kvstore.Replica.OperationAck
import kvstore.Arbiter.Replicas
import kvstore.Replica.Insert
import kvstore.Persister.{PersistFailed, PersistComplete}
import kvstore.GlobalReplicator.{GlobalReplicationTimedOut, GlobalReplicationSuccess, ReplicatorsRemoved}

class PrimaryReplica(persistenceProps: Props) extends Actor {

  var replicateAndPersistId = 0L
  def nextReplicateAndPersistId = {
    val ret = replicateAndPersistId
    replicateAndPersistId += 1
    ret
  }

  val persistenceActor = context.actorOf(persistenceProps)
  context.become(properReceive())

  def receive: Receive = { case _ => }

  def properReceive(
    kv: Map[String, String] = Map.empty[String, String],
    replicasAndReplicators: Map[ActorRef, ActorRef] = Map.empty[ActorRef, ActorRef], // Replica -> Replicator
    pendingPersists: Set[Long] = Set.empty[Long],
    pendingReplications: Map[Long, ActorRef] = Map.empty[Long, ActorRef], // ReplicateAndPersistId -> GlobalReplicator
    originalClients: Map[Long, (Long, ActorRef)] = Map.empty[Long, (Long, ActorRef)]): akka.actor.Actor.Receive = {

    case Insert(key, value, id) =>
      val replicateAndPersistId = nextReplicateAndPersistId

      val newKv = kv + (key -> value)

      context.actorOf(Props(new Persister(new Persist(key, Some(value), replicateAndPersistId), persistenceActor)), "Insert_Persister" + replicateAndPersistId)
      val newPendingPersists = pendingPersists + replicateAndPersistId

      val newPendingReplications =
        if (replicasAndReplicators.nonEmpty) {
          val globalReplicator =
            context.actorOf(Props(new GlobalReplicator(new Replicate(key, Some(value), replicateAndPersistId), replicasAndReplicators.values.toSet)))
          pendingReplications + (replicateAndPersistId -> globalReplicator)
        } else {
          pendingReplications
        }

      val newOriginalClients = originalClients + (replicateAndPersistId -> (id -> sender))

      context.become(properReceive(newKv, replicasAndReplicators, newPendingPersists, newPendingReplications, newOriginalClients))

    case Remove(key, id) =>
      val replicateAndPersistId = nextReplicateAndPersistId

      val newKv = kv - key

      context.actorOf(Props(new Persister(new Persist(key, None, replicateAndPersistId), persistenceActor)), "Remove_Persister" + replicateAndPersistId)
      val newPendingPersists = pendingPersists + replicateAndPersistId

      val newPendingReplications =
        if (replicasAndReplicators.nonEmpty) {
          val globalReplicator =
            context.actorOf(Props(new GlobalReplicator(new Replicate(key, None, replicateAndPersistId), replicasAndReplicators.values.toSet)))
          pendingReplications + (replicateAndPersistId -> globalReplicator)
        } else {
          pendingReplications
        }

      val newOriginalClients = originalClients + (replicateAndPersistId -> (id -> sender))

      context.become(properReceive(newKv, replicasAndReplicators, newPendingPersists, newPendingReplications, newOriginalClients))

    case PersistComplete(key, id) =>
      val newPendingPersists = pendingPersists - id

      // Ensure that we are not still replicating this update.
      val newOriginalClients =
        if (!pendingReplications.contains(id)) {
          originalClients.get(id).foreach {
            case (originalId, originalClient) =>
              originalClient ! OperationAck(originalId)
          }
          originalClients - id
        } else {
          originalClients
        }

      context.become(properReceive(kv, replicasAndReplicators, newPendingPersists, pendingReplications, newOriginalClients))

    case PersistFailed(key, id) =>
      val newPendingPersists = pendingPersists - id

      // Must also update the replications, so that we don't send an ack in the future.
      val newPendingReplications = pendingReplications - id

      originalClients.get(id).foreach {
        case (originalId, originalClient) => originalClient ! OperationFailed(originalId)
      }

      context.become(properReceive(kv, replicasAndReplicators, newPendingPersists, newPendingReplications, originalClients - id))

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(newReplicas) =>
      val newReplicasWithoutSelf = newReplicas.filter( !context.parent.equals(_) )
      val currentReplicas = replicasAndReplicators.keySet
      val addedReplicas = newReplicasWithoutSelf.diff(currentReplicas)
      val removedReplicas = currentReplicas.diff(newReplicasWithoutSelf)

      // We need to
      // 1. Allocate replicators for the newly added replicas
      // 2. Send replication messages to all of the new replicas
      // 3. Stop the replicators that have been removed
      // 4. Send messages to all the current replications telling them about the removed replicas.
      // 5. Update all of the state, and context.become.

      // 1. Allocate replicators for the newly added replicas
      val addedReplicasAndReplicators = addedReplicas.map {
        addedReplica => (addedReplica, context.actorOf(Props(new Replicator(addedReplica))))
      }.toMap


      // 2. Send replication messages to all of the new replicas
      if (addedReplicasAndReplicators.nonEmpty) {
        kv.foreach {
          case (key, value) =>
            val replicateId = nextReplicateAndPersistId
            val replicate = new Replicate(key, Some(value), replicateId)
            context.actorOf(Props(new GlobalReplicator(replicate, addedReplicasAndReplicators.values.toSet)))
        }
      }

      // 3. Stop the replicators that have been removed
      val removedReplicators =
        removedReplicas
          .map(removedRep => replicasAndReplicators.get(removedRep))
          .filter(removedReplicator => removedReplicator.nonEmpty)
          .map(nonEmptyRemovedReplicator => nonEmptyRemovedReplicator.get)

      removedReplicators.foreach(removedReplicator => context.stop(removedReplicator))

      // 4. Send messages to all the current replications telling them about the removed replicas.
      pendingReplications.foreach {
        case (_, globalReplicator) => globalReplicator ! ReplicatorsRemoved(removedReplicators)
      }

      // 5. Update all of the state, and context.become.
      val newReplicasAndReplicators = (replicasAndReplicators ++ addedReplicasAndReplicators) -- removedReplicas
      context.become(properReceive(kv, newReplicasAndReplicators, pendingPersists, pendingReplications, originalClients))

    case GlobalReplicationSuccess(replicate: Replicate) =>
      val newPendingReplications = pendingReplications - replicate.id

      // Ensure that we are not still persisting this update.
      val newOriginalClients =
        if (!pendingPersists.contains(replicate.id)) {
          originalClients.get(replicate.id).foreach {
            case (originalId, originalClient) => originalClient ! OperationAck(originalId)
          }
          originalClients - replicate.id
        } else {
          originalClients
        }

      context.become(properReceive(kv, replicasAndReplicators, pendingPersists, newPendingReplications, newOriginalClients))

    case GlobalReplicationTimedOut(replicate: Replicate) =>
      val newPendingReplications = pendingReplications - replicate.id

      // Must also update the persists, so that we don't send an ack in the future.
      val newPendingPersists = pendingPersists - replicate.id

      originalClients.get(replicate.id).foreach {
        case (originalId, originalClient) => originalClient ! OperationFailed(originalId)
      }

      context.become(properReceive(kv, replicasAndReplicators, newPendingPersists, newPendingReplications, originalClients - replicate.id))

    case _ =>
  }
}